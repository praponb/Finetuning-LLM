WITH CustomerSummary AS (
    SELECT customer_id, MIN(monthly_fee) AS min_monthly_fee, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
    FROM subscriptions
    WHERE status = 'EXPIRED'
    GROUP BY customer_id
)
SELECT
    c.id,
    c.credit_limit,
    c.city,
    C.min_monthly_fee,
    C.stddev_monthly_fee
FROM customers c
LEFT JOIN CustomerSummary C ON c.id = C.customer_id
