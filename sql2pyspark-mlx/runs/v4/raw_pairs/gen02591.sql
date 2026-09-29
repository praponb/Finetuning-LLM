WITH base AS (
    SELECT
        id,
        customer_id,
        monthly_fee,
        status,
        WEEKOFYEAR(end_date) AS end_date_weekofyear
    FROM subscriptions
    WHERE customer_id IS NOT NULL
)
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY monthly_fee DESC, id) AS seq_no,
    AVG(end_date_weekofyear) OVER (PARTITION BY customer_id, status ORDER BY monthly_fee, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS end_date_weekofyear_avg_2,
    AVG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY end_date_weekofyear, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_6
FROM base;
