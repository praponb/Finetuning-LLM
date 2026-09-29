SELECT
    *,
    LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
    ROW_NUMBER() OVER w_hist AS row_seq
FROM subscriptions
WHERE MONTH(start_date) IN (3, 4, 11)
WINDOW w_hist AS (PARTITION BY customer_id ORDER BY end_date, id);
