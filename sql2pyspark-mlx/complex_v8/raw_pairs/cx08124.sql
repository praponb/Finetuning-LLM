WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        INITCAP(TRIM(product)) AS product_clean,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        LENGTH(TRIM(product)) AS product_len
    FROM policies
    WHERE product IN ('TRAVEL', 'HEALTH')
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.approved_amount) FROM claims c WHERE c.policy_id = s.id) AS max_claim_approved_amount,
        (SELECT MIN(c.claimed_amount) FROM claims c WHERE c.policy_id = s.id) AS min_claim_claimed_amount,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.policy_id = s.id) AS first_claim_reported_at
    FROM cleaned_policys s
),
with_activity AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM with_counts s
    WHERE s.holder_id IN (
            SELECT p.holder_id
            FROM policies p
            WHERE p.premium BETWEEN 2013.8 AND 4417.0
              AND p.status IS NOT NULL
        )
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        product,
        premium,
        start_date,
        end_date,
        status,
        product_clean,
        product_len,
        first_claim_reported_at,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY premium_bucket NULLS LAST) AS premium_bucket_rank,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY DATE_TRUNC('MONTH', first_claim_reported_at), holder_id)) AS distinct_status_in_first_claim_reported_at_month,
        MIN(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY premium_bucket NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        ROUND(premium_bucket - AVG(premium_bucket) OVER (PARTITION BY DATE_TRUNC('MONTH', first_claim_reported_at), holder_id), 2) AS premium_bucket_vs_first_claim_reported_at_month_
    FROM with_activity
),
unique_rows AS (
    -- Keep the latest row per product
    SELECT
        id,
        product,
        premium,
        start_date,
        end_date,
        status,
        product_clean,
        product_len,
        first_claim_reported_at,
        premium_bucket_rank,
        distinct_status_in_first_claim_reported_at_month,
        min_premium_so_far,
        premium_bucket_vs_first_claim_reported_at_month_
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY min_premium_so_far DESC, id DESC) AS row_num
        FROM with_trends
    ) ranked
    WHERE row_num = 1
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM unique_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.status IN ('OPEN', 'IN_REVIEW')
              AND c.claim_date IS NOT NULL
        )
),
with_activity_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM without_matches
    WHERE premium_bucket_vs_first_claim_reported_at_month_ < (SELECT MAX(premium_bucket_vs_first_claim_reported_at_month_) FROM without_matches)
)
SELECT
    product_len,
    COALESCE(product, 'ALL') AS product,
    COALESCE(status, 'ALL') AS status,
    SUM(product_len) AS total_product_len,
    ROUND(AVG(premium), 2) AS avg_premium,
    ROUND(100 * COUNT(CASE WHEN status = 'LAPSED' THEN 1 END) / COUNT(*), 1) AS pct_status_lapsed
FROM with_activity_2
GROUP BY ROLLUP(product_len, product, status)
ORDER BY product_len NULLS LAST, product NULLS LAST, status NULLS LAST
