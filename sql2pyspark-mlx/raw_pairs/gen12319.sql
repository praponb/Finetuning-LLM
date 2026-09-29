WITH numbered AS (
    SELECT
        id,
        status,
        monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee, id) AS pos
    FROM subscriptions
)
SELECT id, status, monthly_fee
FROM numbered
WHERE pos <= 1
