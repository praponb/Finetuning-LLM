WITH CustomerSummary AS (
    SELECT customer_id, MAX(end_date) latest_end_date
    FROM subscriptions
    GROUP BY customer_id
)
SELECT c.credit_limit, c.segment, C.latest_end_date
FROM customers c
JOIN CustomerSummary C ON c.id = C.customer_id
ORDER BY c.credit_limit, C.latest_end_date DESC;
