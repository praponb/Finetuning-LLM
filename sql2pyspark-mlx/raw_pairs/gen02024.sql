SELECT
    id,
    plan,
    end_date,
    monthly_fee
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY plan ORDER BY end_date DESC) AS rn
    FROM subscriptions
) AS ranked
WHERE rn = 1
ORDER BY id DESC;
