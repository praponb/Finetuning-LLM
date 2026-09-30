WITH recent_customers AS (
    SELECT *
    FROM customers
    WHERE (country IN ('SG', 'UK', 'TH') AND email LIKE '%@yahoo.com') OR city IS NOT NULL
),
CitySummary AS (
    SELECT city, SUM(CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END) consumer_count
    FROM recent_customers
    GROUP BY city
)
SELECT city, consumer_count
FROM CitySummary
WHERE consumer_count > 71;
