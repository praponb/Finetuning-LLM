WITH filtered AS (
    SELECT
        *,
        LENGTH(segment) AS segment_length,
        INITCAP(TRIM(country)) AS country_title,
        COALESCE(city, 'NONE') AS resolved_city
    FROM customers
    WHERE name NOT LIKE '%Emma%'
)
SELECT *
FROM (
    SELECT
        segment,
        city,
        country,
        credit_limit
    FROM filtered
) src
PIVOT (
    AVG(credit_limit)
    FOR country IN ('JP', 'DE')
)
