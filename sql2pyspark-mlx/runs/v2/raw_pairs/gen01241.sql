WITH OrdersByCustomer AS (
    SELECT customer_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
    FROM orders
    WHERE store_id IS NULL
    GROUP BY customer_id
)
SELECT customers.id, customers.age, OrdersByCustomer.status_values
FROM customers
LEFT JOIN OrdersByCustomer ON customers.id = OrdersByCustomer.customer_id
ORDER BY status_values
