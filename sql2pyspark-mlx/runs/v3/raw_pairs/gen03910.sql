SELECT
    id,
    product_id,
    store_id,
    sale_date,
    revenue,
    LAG(revenue, 1) OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id) AS prev_revenue
FROM store_sales
WHERE store_id IN (1, 4)
