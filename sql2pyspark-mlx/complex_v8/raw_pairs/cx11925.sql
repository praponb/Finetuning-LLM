WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref,
        CASE WHEN premium >= 2945.9 THEN 'large' WHEN premium >= 2013.8 THEN 'medium' ELSE 'small' END AS premium_tier
    FROM policies
    WHERE premium <= 3521.8
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY premium DESC NULLS FIRST, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium ASC), 3) AS premium_percent_rank
    FROM base_policys
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        s.days_start_date_to_end_date,
        s.product_ref,
        s.premium_tier,
        s.premium_percent_rank,
        c.id AS claim_id,
        c.claim_date,
        c.reported_at AS claim_reported_at
    FROM windowed s
    LEFT JOIN claims c ON c.policy_id = s.id AND HOUR(c.reported_at) BETWEEN 8 AND 10
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        premium,
        days_start_date_to_end_date,
        premium_percent_rank,
        claim_id,
        claim_date,
        claim_reported_at,
        DAYOFWEEK(end_date) AS end_date_dayofweek
    FROM detail_rows
)
-- Final projection
SELECT DISTINCT id, premium_percent_rank
FROM derived
