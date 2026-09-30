WITH valid_customers AS (
    SELECT *, DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_date_label
    FROM customers
    WHERE city = 'Chiang Mai'
),
summary AS (
    SELECT segment, ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit, ROUND(AVG(age), 2) AS avg_age
    FROM valid_customers
    GROUP BY segment
)
SELECT *
FROM summary
ORDER BY segment DESC
