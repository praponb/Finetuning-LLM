SELECT
    id,
    product_id,
    sale_date,
    revenue,
    units,
    LAG(revenue, 1, 0) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS prev_revenue,
    RANK() OVER (PARTITION BY product_id ORDER BY units DESC) AS rnk
FROM store_sales;
