SELECT
    id,
    product_id,
    sale_date,
    revenue,
    LEAD(revenue, 1, 0) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS next_revenue
FROM store_sales
WHERE store_id IN (1, 2, 3, 6)
ORDER BY id;
