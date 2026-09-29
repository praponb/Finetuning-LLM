WITH OrdersByCustomer AS (
    SELECT customer_id, MIN(discount) AS min_discount
    FROM orders
    WHERE store_id = 18
    GROUP BY customer_id
)
SELECT c.id, c.city, O.min_discount
FROM customers c
INNER JOIN OrdersByCustomer O ON c.id = O.customer_id
WHERE O.min_discount >= 0.39
ORDER BY c.id DESC, min_discount
