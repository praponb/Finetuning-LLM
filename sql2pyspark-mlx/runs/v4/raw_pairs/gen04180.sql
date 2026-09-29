WITH base AS (
    SELECT
        id,
        plan,
        end_date,
        monthly_fee,
        status,
        DAYOFWEEK(end_date) end_date_dayofweek
    FROM subscriptions
    WHERE start_date > DATE '2024-03-25'
      AND customer_id IS NOT NULL
      AND end_date BETWEEN '2024-06-01' AND '2024-09-01'
)
SELECT
    plan,
    SUM(CASE WHEN plan = 'premium' THEN monthly_fee ELSE 0 END) AS premium_monthly_fee,
    SUM(CASE WHEN status = 'EXPIRED' THEN end_date_dayofweek ELSE 0 END) AS expired_end_date_dayofweek,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    ROUND(SUM(end_date_dayofweek) / NULLIF(COUNT(CASE WHEN plan = 'premium' THEN 1 END), 0), 2) AS end_date_dayofweek_per_premium,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END), 0), 2) AS monthly_fee_per_active,
    COUNT_IF(plan = 'premium') AS premium_events
FROM base
GROUP BY CUBE (plan)
ORDER BY expired_end_date_dayofweek DESC
