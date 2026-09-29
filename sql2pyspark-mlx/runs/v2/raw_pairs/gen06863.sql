WITH src AS (
    SELECT *, LAST_DAY(start_date) AS start_date_month_end
    FROM subscriptions
    WHERE (customer_id IN (5, 20, 21, 36) AND customer_id = 35) OR customer_id IN (16, 20, 28, 30)
),
summary AS (
    SELECT plan, SORT_ARRAY(COLLECT_SET(status)) AS status_list, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
    FROM src
    GROUP BY plan
)
SELECT plan, status_list, stddev_monthly_fee
FROM summary
WHERE stddev_monthly_fee >= 21
ORDER BY plan, stddev_monthly_fee DESC
