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
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        DATEDIFF(DATE '2025-06-30', start_date) AS days_since_start_date_2
    FROM policies
),
stats AS (
    -- Group-level totals
    SELECT
        status,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list
    FROM valid
    GROUP BY status, DATE_FORMAT(end_date, 'yyyy-MM')
    HAVING MAX(premium) > 1051.9
)
SELECT
    status,
    product_list,
    CONCAT(end_date_ym, '#', CAST(row_count AS STRING)) AS end_date_ym_ref
FROM stats
ORDER BY end_date_ym_ref DESC, status
