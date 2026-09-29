WITH enriched AS (
    SELECT
        s.*,
        b.email,
        b.city,
        b.credit_limit
    FROM subscriptions s
    JOIN customers b
        ON s.customer_id = b.id
)
SELECT
    DATE_TRUNC('MONTH', end_date) AS activity_month,
    PERCENTILE_APPROX(monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_quartiles,
    COUNT(CASE WHEN status = 'PAUSED' THEN 1 END) AS paused_count,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    MAX(credit_limit) AS max_credit_limit,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    GROUPING(DATE_TRUNC('MONTH', end_date)) AS grouping_id
FROM enriched
GROUP BY ROLLUP (DATE_TRUNC('MONTH', end_date));
