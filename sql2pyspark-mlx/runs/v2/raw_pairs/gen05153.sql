WITH Ranked AS (
    SELECT
        id,
        customer_id,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, id) AS row_num
    FROM orders
)
SELECT *
FROM Ranked
WHERE row_num <= 2
ORDER BY row_num
