WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date
    FROM policies
    WHERE status IS NOT NULL
      AND holder_id IS NOT NULL
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.days_end_date_to_start_date,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claim_date,
        c.claimed_amount AS claim_claimed_amount,
        c.status AS claim_status
    FROM scoped_policys s
    LEFT JOIN claims c ON c.policy_id = s.id AND c.status <> 'PAID'
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(end_date, 'yyyy-MM-dd') AS end_date_label,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM exploded_detail_claims
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(premium, claim_claimed_amount) AS greatest_premium_claim_claimed_amount
    FROM derived
),
deduped AS (
    -- Keep the earliest row per holder_id, product
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        days_end_date_to_start_date,
        claim_id,
        claim_approved_amount,
        claim_date,
        claim_claimed_amount,
        claim_status,
        end_date_label,
        product_slug,
        greatest_premium_claim_claimed_amount
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, product ORDER BY days_end_date_to_start_date, id, claim_id) AS row_num
        FROM labeled
    ) ranked
    WHERE row_num = 1
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        claim_id,
        COUNT(*) AS cnt,
        ROUND(AVG(premium), 2) AS avg_premium,
        PERCENTILE_APPROX(days_end_date_to_start_date, 0.5) AS median_days_end_date_to_start_date,
        ROUND(SUM(claim_approved_amount) / NULLIF(SUM(premium), 0), 3) AS claim_approved_amount_to_premium_ratio,
        COUNT(DISTINCT claim_status) AS distinct_claim_status,
        ROUND(STDDEV(premium), 2) AS stddev_premium
    FROM deduped
    GROUP BY 1
    HAVING AVG(greatest_premium_claim_claimed_amount) > 20060.0
),
tagged AS (
    -- Derive helper columns
    SELECT
        claim_id,
        cnt,
        claim_approved_amount_to_premium_ratio,
        distinct_claim_status,
        stddev_premium,
        FLOOR(stddev_premium / 1000) * 1000 AS stddev_premium_bucket,
        CASE WHEN distinct_claim_status >= 7 THEN 'A' WHEN distinct_claim_status >= 3 THEN 'B' ELSE 'C' END AS distinct_claim_status_tier
    FROM totals
    WHERE cnt > 2
)
-- Final projection
SELECT
    claim_id,
    cnt,
    claim_approved_amount_to_premium_ratio,
    distinct_claim_status,
    stddev_premium
FROM tagged
ORDER BY claim_approved_amount_to_premium_ratio DESC, claim_id
LIMIT 5
