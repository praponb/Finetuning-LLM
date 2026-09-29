WITH src AS (
    SELECT *
    FROM suppliers
    WHERE (country = 'TH' AND supplier_name IS NOT NULL) OR UPPER(TRIM(supplier_name)) = 'GLOBAL SUPPLY'
),
summary AS (
    SELECT CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) AS bangkok_count, SORT_ARRAY(COLLECT_SET(country)) AS country_list
    FROM src
    GROUP BY CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END
)
SELECT rating_bucket, bangkok_count, country_list
FROM summary;
