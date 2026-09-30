WITH cleaned AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date,
        COALESCE(city, 'NONE') AS resolved_city
    FROM customers
    WHERE signup_date >= DATE '2023-08-03'
      AND city IN ('New York', 'Tokyo', 'Singapore')
)
SELECT
    *,
    AVG(days_since_signup_date) OVER (PARTITION BY DATE_TRUNC('month', signup_date), country ORDER BY days_since_signup_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS days_since_signup_date_avg_3_prior,
    AVG(age) OVER (PARTITION BY YEAR(signup_date), city ORDER BY age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS age_avg_6,
    STDDEV_SAMP(credit_limit) OVER (PARTITION BY country) AS country_stddev_samp_credit_limit
FROM cleaned
WHERE signup_date <= '2025-05-01';
