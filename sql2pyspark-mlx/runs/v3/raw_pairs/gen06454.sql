WITH filtered_customers AS (
    SELECT *
    FROM customers
    WHERE credit_limit > 15100
),
SegmentSummary AS (
    SELECT
        segment,
        YEAR(signup_date) AS signup_year,
        MAX(age) AS max_age,
        SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) AS bangkok_count
    FROM filtered_customers
    GROUP BY segment, YEAR(signup_date)
)
SELECT
    segment,
    signup_year,
    max_age,
    bangkok_count
FROM SegmentSummary
WHERE bangkok_count < 73
ORDER BY signup_year, segment
LIMIT 5;
