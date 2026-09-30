SELECT
    product_id,
    SUM(quantity) AS total_quantity,
    COUNT(*) AS cnt,
    RANK() OVER (ORDER BY SUM(quantity) DESC) AS rnk
FROM inventory
WHERE store_id = 3
GROUP BY product_id;
