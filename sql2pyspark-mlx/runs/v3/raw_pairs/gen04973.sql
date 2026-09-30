WITH customer_totals AS (
    SELECT customer_id, SUM(CASE WHEN plan = 'basic' THEN monthly_fee ELSE 0 END) AS basic_monthly_fee
    FROM subscriptions
    GROUP BY customer_id
)
SELECT customers.id, customers.email, customer_totals.basic_monthly_fee
FROM customers
LEFT JOIN customer_totals ON customers.id = customer_totals.customer_id
WHERE customer_totals.basic_monthly_fee > 57
ORDER BY id DESC;
