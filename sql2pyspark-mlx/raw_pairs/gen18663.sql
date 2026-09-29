WITH OrdersByCustomer AS (
    SELECT customer_id, MAX(shipping_fee) - MIN(shipping_fee) AS shipping_fee_range
    FROM orders
    GROUP BY customer_id
)
SELECT c.id, c.name, O.shipping_fee_range
FROM customers c
INNER JOIN OrdersByCustomer O ON c.id = O.customer_id;
