SELECT
    id,
    store_id,
    sale_date,
    revenue,
    revenue - LAG(revenue, 2) OVER (PARTITION BY store_id ORDER BY sale_date, id) revenue_change
FROM store_sales
WHERE store_id IN (2, 3, 4, 6);
