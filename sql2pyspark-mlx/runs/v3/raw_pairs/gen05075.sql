SELECT
    id,
    status,
    monthly_fee,
    end_date
FROM (
    SELECT
        id,
        status,
        monthly_fee,
        end_date,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY monthly_fee) AS pos
    FROM subscriptions
) AS t
WHERE pos <= 3;
