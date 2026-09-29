SELECT
    id,
    credit_limit,
    is_active,
    segment,
    CONCAT(country, '/', segment) AS country_segment,
    SUBSTRING(segment, 1, 2) AS segment_prefix
FROM customers
WHERE MONTH(signup_date) IN (2, 3, 4) AND name NOT LIKE '%Chen%' AND city NOT IN ('Bangkok', 'New York')
ORDER BY id DESC, segment_prefix
LIMIT 20
