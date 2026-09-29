WITH base AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        LOWER(TRIM(product)) AS product_clean,
        CASE WHEN product IN ('LIFE', 'HEALTH') THEN 'group_a' WHEN product = 'TRAVEL' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE premium >= 2319.8
      AND status = 'LAPSED'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        premium,
        start_date,
        end_date,
        premium_bucket,
        product_clean,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id ORDER BY premium_bucket), 3) AS premium_bucket_cume_dist
    FROM base
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM sequenced s
    WHERE s.id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.claim_type <> 'fire'
        )
      AND s.premium_bucket >= 1640
)
-- Final projection
SELECT
    id,
    premium,
    end_date,
    premium_bucket_cume_dist,
    UPPER(TRIM(product_clean)) AS product_clean_clean,
    UPPER(TRIM(product_clean)) AS product_clean_clean_2
FROM screened
