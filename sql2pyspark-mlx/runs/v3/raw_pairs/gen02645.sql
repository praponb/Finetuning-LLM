SELECT *
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY quantity) AS pos
    FROM order_items
) AS t
WHERE pos <= 5
ORDER BY product_id ASC;
