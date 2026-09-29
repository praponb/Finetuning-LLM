WITH valid AS (
    SELECT *, ROUND(monthly_fee * 1.07, 2) AS adjusted_monthly_fee
    FROM subscriptions
    WHERE end_date > DATE '2023-07-15'
      AND customer_id IS NOT NULL
)
SELECT
    a.id,
    a.customer_id,
    a.plan,
    a.status,
    a.adjusted_monthly_fee,
    p.age,
    p.email
FROM valid a
INNER JOIN customers p
    ON a.customer_id = p.id
