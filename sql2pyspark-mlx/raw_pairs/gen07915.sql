WITH cleaned AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date,
        DATE_TRUNC('week', signup_date) AS signup_date_period,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM customers
    WHERE signup_date >= TO_DATE('2023-04-20')
      AND segment IS NOT NULL
)
SELECT
    CASE WHEN age >= 72 THEN 'LARGE' ELSE 'SMALL' END AS age_tier,
    city,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(*) AS row_count,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN age >= 72 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(*) DESC) AS row_count_dense_rank
FROM cleaned
GROUP BY age_tier, city
ORDER BY city DESC NULLS LAST;
