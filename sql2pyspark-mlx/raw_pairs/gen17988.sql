WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY order_date DESC) AS rnk
    FROM orders
)
SELECT *
FROM numbered
WHERE rnk = 1
