WITH recent_policys AS (
    -- Scope the policies rows
    SELECT *
    FROM policies
    WHERE holder_id >= 16
      AND start_date >= DATE '2024-06-19'
),
screened AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM recent_policys s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium < 3465.4
              AND p.product IS NOT NULL
        )
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, p.region AS holder_region
    FROM screened s
    JOIN policyholders p ON p.id = s.holder_id
),
per_group AS (
    -- Summarise per group
    SELECT
        product,
        status,
        MIN(premium) AS min_premium,
        MAX(end_date) AS last_end_date,
        ROUND(SUM(premium), 2) AS total_premium,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE status = 'ACTIVE') AS n_status_active
    FROM enriched
    GROUP BY product, status
    HAVING SUM(premium) > 1042.8
)
-- Final projection
SELECT
    product,
    status,
    min_premium,
    last_end_date,
    total_premium,
    n_status_active
FROM per_group
WHERE status NOT IN ('LAPSED', 'ACTIVE')
ORDER BY status DESC NULLS FIRST, product NULLS LAST
