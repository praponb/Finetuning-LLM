SELECT
    DATE_TRUNC('YEAR', signup_date) AS year_start,
    segment,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    SIZE(COLLECT_SET(country)) AS num_countries,
    COUNT(DISTINCT city) AS distinct_cities,
    BIT_AND(age) AS age_bit_and
FROM customers
GROUP BY DATE_TRUNC('YEAR', signup_date), segment
ORDER BY segment DESC NULLS LAST;
