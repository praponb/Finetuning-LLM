WITH recent_products AS (
    SELECT *
    FROM products
    WHERE supplier_id = 9 AND supplier_id IN (21, 23, 24) AND launch_date < '2024-03-01'
),
supplier_id_summary AS (
    SELECT
        supplier_id,
        DATE_FORMAT(launch_date, 'yyyy-MM') launch_month,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) category_values,
        MIN(cost) min_cost
    FROM recent_products
    GROUP BY supplier_id, DATE_FORMAT(launch_date, 'yyyy-MM')
)
SELECT *
FROM supplier_id_summary
ORDER BY supplier_id DESC, launch_month;
