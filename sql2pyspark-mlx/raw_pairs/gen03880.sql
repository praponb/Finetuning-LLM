SELECT
    id,
    store_id,
    product_id,
    units,
    revenue,
    AVG(units) OVER w_ordered AS last7_avg_units,
    COUNT(units) OVER w_ordered AS last7_count_units,
    MIN(units) OVER w_ordered AS last7_min_units
FROM store_sales
WHERE product_id IN (3, 5, 10)
WINDOW w_ordered AS (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
