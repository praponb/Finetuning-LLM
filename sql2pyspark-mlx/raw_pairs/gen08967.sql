SELECT
    id,
    sale_date,
    revenue,
    LEAD(revenue, 1) OVER (ORDER BY sale_date, id) AS next_revenue
FROM store_sales
WHERE store_id IN (1, 2, 3, 5);
