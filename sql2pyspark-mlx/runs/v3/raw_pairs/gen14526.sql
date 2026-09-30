SELECT
    id,
    end_date,
    customer_id,
    start_date,
    monthly_fee,
    NTILE(3) OVER (ORDER BY end_date DESC, id) AS end_date_ntile,
    monthly_fee - LAG(monthly_fee, 2) OVER (PARTITION BY customer_id ORDER BY start_date, id) AS monthly_fee_change
FROM subscriptions
WHERE monthly_fee <= 180;
