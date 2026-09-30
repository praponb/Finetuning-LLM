SELECT id, product_id, quantity
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS rnk
    FROM order_items
) AS t
WHERE rnk <= 3
ORDER BY id
