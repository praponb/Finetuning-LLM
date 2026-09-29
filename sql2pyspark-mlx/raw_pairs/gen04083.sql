WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        LENGTH(city) AS city_length,
        CASE WHEN age >= 67 THEN 'High' WHEN age >= 38 THEN 'Medium' ELSE 'Low' END AS age_tier
    FROM customers
    WHERE signup_date > DATE '2023-02-13'
      AND segment IS NOT NULL
)
-- Ranking and gap features
SELECT
    id,
    age,
    age_tier,
    SUM(city_length) OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_city_length,
    NTILE(3) OVER (PARTITION BY country ORDER BY age, id) AS age_bucket
FROM filtered
