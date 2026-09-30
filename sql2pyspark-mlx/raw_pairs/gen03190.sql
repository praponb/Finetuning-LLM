SELECT
    customer_id,
    plan,
    DATE_FORMAT(end_date, 'yyyy-MM') end_month,
    MAX(end_date) last_end_date
FROM subscriptions
GROUP BY customer_id, plan, DATE_FORMAT(end_date, 'yyyy-MM')
