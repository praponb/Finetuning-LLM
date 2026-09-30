WITH CustomerSummary AS (
    SELECT customer_id, SUM(CASE WHEN status = 'PAUSED' THEN monthly_fee ELSE 0 END) paused_monthly_fee
    FROM subscriptions
    GROUP BY customer_id
)
SELECT c.name, C.paused_monthly_fee
FROM customers c
LEFT OUTER JOIN CustomerSummary C ON c.id = C.customer_id
WHERE C.paused_monthly_fee > 73
ORDER BY name DESC;
