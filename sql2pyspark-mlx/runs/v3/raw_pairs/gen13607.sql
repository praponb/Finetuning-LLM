SELECT
    product_id,
    SUM(quantity) AS total_quantity,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(quantity) DESC) AS rnk
FROM inventory
GROUP BY product_id
