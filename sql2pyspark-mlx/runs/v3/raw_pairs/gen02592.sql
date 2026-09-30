WITH CustomerTotals AS (
    SELECT customer_id, COUNT(*) AS subscription_count
    FROM subscriptions
    GROUP BY customer_id
)
SELECT c.is_active, c.city, C.subscription_count
FROM customers c
INNER JOIN CustomerTotals C ON c.id = C.customer_id
ORDER BY 3 DESC, city
