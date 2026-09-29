WITH filtered_customers AS (
    SELECT *
    FROM customers
    WHERE city = 'Tokyo' AND (LOWER(TRIM(name)) = 'carol diaz' OR email IS NOT NULL)
),
agg AS (
    SELECT country, YEAR(signup_date) AS signup_year, SORT_ARRAY(COLLECT_SET(segment)) AS segment_list
    FROM filtered_customers
    GROUP BY country, YEAR(signup_date)
)
SELECT *
FROM agg
ORDER BY country, signup_year DESC
LIMIT 1;
