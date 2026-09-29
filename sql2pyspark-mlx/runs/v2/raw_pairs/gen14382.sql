SELECT status, customer_id, min_monthly_fee
FROM (
    SELECT status, customer_id, MIN(monthly_fee) min_monthly_fee
    FROM subscriptions
    GROUP BY status, customer_id
) agg
WHERE min_monthly_fee >= 220
ORDER BY 3 DESC, customer_id DESC;
