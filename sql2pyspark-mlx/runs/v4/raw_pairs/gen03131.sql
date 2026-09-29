SELECT *
FROM (
    SELECT
        customer_id,
        status,
        monthly_fee
    FROM subscriptions
) src
PIVOT (
    SUM(monthly_fee)
    FOR status IN ('EXPIRED', 'PAUSED')
);
