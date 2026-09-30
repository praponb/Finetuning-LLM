WITH base AS (
    SELECT *, DATEDIFF(start_date, end_date) / 7 AS weeks_end_date_to_start_date
    FROM subscriptions
    WHERE customer_id IS NOT NULL
)
SELECT
    customer_id,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN status = 'PAUSED' THEN 1 END), 0), 2) AS monthly_fee_per_paused,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    ROUND(AVG(weeks_end_date_to_start_date), 2) AS avg_weeks_end_date_to_start_date
FROM base
GROUP BY customer_id
ORDER BY num_status;
