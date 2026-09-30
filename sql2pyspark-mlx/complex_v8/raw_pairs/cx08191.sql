WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        INITCAP(TRIM(product)) AS product_clean,
        INITCAP(TRIM(status)) AS status_clean,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label
    FROM policies
    WHERE premium <= 4670.9
      AND start_date < DATE '2025-03-21'
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, COUNT(*) AS policy_count, COUNT(*) AS policy_count_2
    FROM policies
    GROUP BY holder_id
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count,
        COALESCE(p.policy_count_2, 0) AS policy_count_2
    FROM valid s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
without_matches AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_counts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.start_date < DATE '2025-02-14'
              AND p.status IN ('EXPIRED', 'ACTIVE')
        )
      AND s.start_date_label IS NOT NULL
),
joined_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.status,
        s.product_clean,
        s.policy_count,
        s.policy_count_2,
        p.region AS holder_region,
        p.holder_name
    FROM without_matches s
    LEFT JOIN policyholders p ON p.id = s.holder_id AND p.risk_score < 0.8
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        product_clean,
        policy_count,
        holder_region,
        holder_name,
        LEAD(start_date, 3) OVER w_status_policy_count_by_premi AS next3_start_date,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY status, policy_count ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), holder_id ORDER BY policy_count_2 ASC, id) AS rn,
        ROUND(policy_count / NULLIF(SUM(policy_count) OVER (PARTITION BY product), 0), 4) AS policy_count_share_of_product,
        LAG(policy_count_2) OVER (PARTITION BY holder_id, status ORDER BY policy_count DESC, id) AS prev_policy_count_2,
        MAX(premium) OVER (PARTITION BY status, policy_count ORDER BY premium DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_max_3_prior
    FROM joined_holder
    WINDOW w_status_policy_count_by_premi AS (PARTITION BY status, policy_count ORDER BY premium DESC, id)
)
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    product_clean,
    policy_count,
    holder_region,
    holder_name,
    rn,
    policy_count_share_of_product,
    prev_policy_count_2,
    premium_max_3_prior,
    CONCAT(holder_name, '#', CAST(id AS STRING)) AS holder_name_ref
FROM sequenced
WHERE LOWER(holder_name) LIKE '%no%'
ORDER BY start_date NULLS LAST, prev_policy_count_2 NULLS LAST
