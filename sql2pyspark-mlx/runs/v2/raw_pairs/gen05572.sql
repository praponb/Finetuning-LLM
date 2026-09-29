WITH CustomerStats AS (
    SELECT customer_id, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
    FROM subscriptions
    GROUP BY customer_id
)
SELECT c.id, c.is_active, C.stddev_monthly_fee
FROM customers c
INNER JOIN CustomerStats C ON c.id = C.customer_id
WHERE C.stddev_monthly_fee > 39
ORDER BY C.stddev_monthly_fee DESC;
