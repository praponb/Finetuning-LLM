SELECT
    segment,
    DATE_TRUNC('YEAR', signup_date) AS year_start,
    COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count,
    COUNT(DISTINCT city) AS distinct_cities,
    COUNT(*) AS num_rows
FROM customers
GROUP BY CUBE (segment, DATE_TRUNC('YEAR', signup_date));
