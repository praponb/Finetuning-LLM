WITH FilteredSubscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE MONTH(start_date) IN (4, 8, 12)
),
Agg AS (
    SELECT
        QUARTER(start_date) AS start_quarter,
        CASE WHEN plan IN ('enterprise', 'standard') THEN 'core' ELSE 'other' END AS plan_group,
        ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
        ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
    FROM FilteredSubscriptions
    GROUP BY QUARTER(start_date), CASE WHEN plan IN ('enterprise', 'standard') THEN 'core' ELSE 'other' END
)
SELECT *
FROM Agg
WHERE avg_monthly_fee >= 21
ORDER BY avg_monthly_fee;
