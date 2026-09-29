WITH base AS (
    SELECT *, CASE WHEN city IN ('Chiang Mai', 'New York') THEN 'Tier 1' ELSE 'Other' END AS city_group
    FROM suppliers
    WHERE city IS DISTINCT FROM 'Tokyo'
      AND supplier_name LIKE '%Wholesale%'
),
scored AS (
    SELECT
        id,
        country,
        city,
        rating,
        city_group,
        CUME_DIST() OVER (PARTITION BY country ORDER BY rating DESC NULLS LAST) AS rating_cume_dist,
        CUME_DIST() OVER (PARTITION BY city, country ORDER BY rating DESC NULLS FIRST) AS rating_cume_dist_2,
        AVG(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rating_avg_3
    FROM base
),
ranked AS (
    SELECT
        id,
        country,
        city,
        rating_cume_dist_2,
        rating_avg_3
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating_cume_dist DESC NULLS LAST, id) AS rank_in_group
        FROM scored
    ) x
    WHERE rank_in_group = 1
),
remaining_keys AS (
    SELECT city
    FROM ranked
    EXCEPT
    SELECT city
    FROM suppliers
    WHERE city IN ('Singapore', 'New York', 'Bangkok')
),
kept_rows AS (
    SELECT s.*
    FROM ranked s
    INNER JOIN remaining_keys k ON s.city = k.city
)
SELECT
    country,
    MAX(rating_avg_3) FILTER (WHERE country = 'JP') AS jp_max_rating_avg_3,
    PERCENTILE_APPROX(rating_avg_3, 0.5) AS rating_avg_3_p50,
    ROUND(AVG(rating_avg_3), 2) AS avg_rating_avg_3,
    COUNT_IF(country = 'JP') AS jp_events,
    ROUND(SUM(rating_avg_3) / NULLIF(COUNT(CASE WHEN city = 'Tokyo' THEN 1 END), 0), 2) AS rating_avg_3_per_tokyo,
    COUNT(*) FILTER (WHERE city = 'Berlin') AS berlin_rows
FROM kept_rows
GROUP BY country
ORDER BY country NULLS FIRST
