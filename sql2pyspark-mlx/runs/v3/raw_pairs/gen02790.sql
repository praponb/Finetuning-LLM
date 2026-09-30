WITH src AS (
    SELECT *, ROUND(rating, 0) rating_rounded
    FROM suppliers
    WHERE (rating BETWEEN 2.1 AND 3.3 AND city NOT IN ('Bangkok', 'New York', 'London')) OR country IN ('JP', 'US', 'UK')
),
rating_rounded_bucket_summary AS (
    SELECT CASE WHEN rating_rounded >= 81 THEN 'high' WHEN rating_rounded >= 19 THEN 'mid' ELSE 'low' END rating_rounded_bucket, SORT_ARRAY(COLLECT_SET(country)) country_list
    FROM src
    GROUP BY CASE WHEN rating_rounded >= 81 THEN 'high' WHEN rating_rounded >= 19 THEN 'mid' ELSE 'low' END
)
SELECT rating_rounded_bucket, country_list
FROM rating_rounded_bucket_summary
