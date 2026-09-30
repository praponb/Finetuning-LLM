WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) AS rank_in_group
    FROM subscriptions
    QUALIFY rank_in_group = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', end_date) AS year_start,
    SUM(monthly_fee) FILTER (WHERE plan = 'premium') AS premium_total_monthly_fee,
    COUNT_IF(status = 'PAUSED') AS paused_events,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM latest
GROUP BY DATE_TRUNC('YEAR', end_date)
