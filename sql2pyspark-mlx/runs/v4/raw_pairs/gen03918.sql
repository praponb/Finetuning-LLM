SELECT
    *,
    ROW_NUMBER() OVER w_part AS seq_no,
    AVG(monthly_fee) OVER (ORDER BY monthly_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_3
FROM subscriptions
WINDOW w_part AS (PARTITION BY plan ORDER BY end_date, id);
