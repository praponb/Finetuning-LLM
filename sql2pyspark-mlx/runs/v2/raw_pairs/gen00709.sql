WITH FilteredSubscriptions AS (
    SELECT *, CASE WHEN plan IN ('premium', 'standard') THEN 'Group A' ELSE 'Other' END AS plan_group
    FROM subscriptions
    WHERE customer_id IN (13, 15, 17, 28)
),
start_year_summary AS (
    SELECT YEAR(start_date) AS start_year, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
    FROM FilteredSubscriptions
    GROUP BY YEAR(start_date)
)
SELECT *
FROM start_year_summary
WHERE stddev_monthly_fee >= 75;
