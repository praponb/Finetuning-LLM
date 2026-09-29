WITH cleaned AS (
    SELECT
        *,
        UPPER(city) AS city_upper,
        CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END AS is_singapore
    FROM suppliers
    WHERE country = 'UK'
)
SELECT *, RANK() OVER (PARTITION BY city, country ORDER BY is_singapore DESC NULLS FIRST) AS latest_rank
FROM cleaned
QUALIFY latest_rank = 1;
