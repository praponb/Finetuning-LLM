WITH customer_totals AS (
    SELECT customer_id, ROUND(STDDEV(shipping_fee), 2) AS stddev_shipping_fee, COUNT(amount) AS amount_count
    FROM orders
    GROUP BY customer_id
)
SELECT
    customers.credit_limit,
    customers.email,
    customer_totals.stddev_shipping_fee,
    customer_totals.amount_count
FROM customers
INNER JOIN customer_totals ON customers.id = customer_totals.customer_id
ORDER BY 3 DESC;
