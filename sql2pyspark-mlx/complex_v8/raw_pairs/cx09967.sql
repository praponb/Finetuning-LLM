WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM policies
    WHERE holder_id IS NOT NULL
      AND product IN ('TRAVEL', 'HOME')
),
screened AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND LOWER(p.holder_name) LIKE '%wo%'
              AND p.joined_on >= DATE '2024-07-18'
        )
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        product,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT status) AS distinct_status,
        COUNT(DISTINCT id) AS distinct_ids
    FROM screened
    WHERE product_slug IS NOT NULL
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
)
SELECT *
FROM (
    SELECT n_rows, product, distinct_status
    FROM rollup_base
) src
PIVOT (
    MAX(distinct_status)
    FOR product IN ('HEALTH', 'LIFE', 'AUTO', 'HOME')
)
ORDER BY n_rows
