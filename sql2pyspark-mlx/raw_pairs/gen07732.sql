SELECT
    id,
    product_id,
    units,
    revenue,
    ROUND(units / SUM(units) OVER (PARTITION BY product_id), 3) AS units_share,
    RANK() OVER (ORDER BY revenue DESC) AS rnk
FROM store_sales
WHERE product_id IN (2, 3, 5, 7)
ORDER BY product_id DESC, revenue DESC
