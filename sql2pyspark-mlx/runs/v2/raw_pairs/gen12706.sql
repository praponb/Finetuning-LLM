SELECT *
FROM (
    SELECT customer_id, COUNT(DISTINCT amount) unique_customers
    FROM orders
    WHERE channel <> 'store'
    GROUP BY customer_id
) x
WHERE unique_customers >= 4
