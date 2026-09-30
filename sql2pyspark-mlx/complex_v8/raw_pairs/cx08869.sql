WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LOWER(TRIM(status)) AS status_clean
    FROM policies
),
screened AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.region NOT IN ('East', 'South')
        )
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        DATE_ADD(start_date, 30) AS start_date_plus_30d,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date
    FROM screened
)
SELECT
    COALESCE(product, 'ALL') AS product,
    holder_id,
    COALESCE(status, 'ALL') AS status,
    ROUND(STDDEV(premium), 2) AS stddev_premium
FROM labeled
GROUP BY GROUPING SETS ((product, holder_id, status), (product))
ORDER BY product NULLS LAST, holder_id NULLS LAST, status NULLS LAST
