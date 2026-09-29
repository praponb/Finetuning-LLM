WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE start_date BETWEEN DATE '2024-07-19' AND DATE '2025-08-03'
),
summary AS (
    -- Summarise per group
    SELECT
        holder_id,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        SUM(CASE WHEN product IN ('HEALTH', 'LIFE') THEN 1 ELSE 0 END) AS n_product_in_health_life,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT status) AS distinct_status
    FROM valid
    WHERE holder_id IS NOT NULL
    GROUP BY 1
)
SELECT
    holder_id,
    n_rows,
    n_product_in_health_life,
    distinct_ids,
    distinct_status,
    n_rows - distinct_status AS n_rows_minus_distinct_status
FROM summary
ORDER BY n_rows
