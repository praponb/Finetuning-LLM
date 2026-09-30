WITH base AS (
    SELECT
        *,
        LENGTH(city) AS city_length,
        COALESCE(city, 'N/A') AS resolved_city
    FROM suppliers
    WHERE rating > 1.9
),
enriched AS (
    SELECT
        *,
        LEAD(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) AS next_rating,
        LAG(city_length, 1, 0) OVER (ORDER BY rating, id) AS prev_city_length,
        ROW_NUMBER() OVER (ORDER BY city_length, id) AS event_seq
    FROM base
),
with_windows AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        city_length,
        resolved_city,
        next_rating,
        prev_city_length,
        event_seq,
        LAG(prev_city_length, 2) OVER (PARTITION BY country ORDER BY prev_city_length, id) AS prev_prev_city_length,
        LEAD(rating, 1) OVER (ORDER BY next_rating, id) AS next_rating_2
    FROM enriched
),
summary AS (
    SELECT
        CASE WHEN event_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
        ROUND(AVG(rating), 2) AS avg_rating,
        COUNT(*) FILTER (WHERE country = 'US') AS us_rows,
        COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count
    FROM with_windows
    GROUP BY CASE WHEN event_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    event_seq_tier,
    avg_rating,
    us_rows,
    uk_count
FROM summary
ORDER BY event_seq_tier DESC NULLS LAST, us_rows DESC
LIMIT 10;
