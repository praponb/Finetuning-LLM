WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        LENGTH(city) AS city_length,
        CASE WHEN rating >= 3.9 THEN 'High' WHEN rating >= 2.4 THEN 'Medium' ELSE 'Low' END AS rating_tier
    FROM suppliers
    WHERE city IS NOT NULL
      AND rating BETWEEN 2.1 AND 2.2
)
-- Running and moving aggregates
SELECT
    id,
    city_length,
    LEAD(city_length, 1) OVER (ORDER BY city_length, id) AS next_city_length,
    AVG(city_length) OVER (ORDER BY city_length, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS city_length_avg_2_prior,
    ROW_NUMBER() OVER (ORDER BY city_length DESC, id) AS seq_no
FROM base;
