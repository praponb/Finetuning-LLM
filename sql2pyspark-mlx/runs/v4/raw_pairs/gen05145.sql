WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        NTILE(4) OVER w_grp AS rating_bucket,
        LAG(rating, 1) OVER (ORDER BY rating, id) AS prev_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        AVG(rating) OVER w_hist AS rating_avg_5
    FROM suppliers
    WINDOW w_grp AS (PARTITION BY city ORDER BY rating DESC, id), w_hist AS (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        rating_bucket,
        rating_share,
        rating_avg_5,
        SUBSTRING(country, 1, 4) AS country_prefix,
        COALESCE(city, 'N/A') AS city_clean
    FROM with_windows
    WHERE city IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        country,
        rating_bucket,
        rating_share,
        city_clean,
        CEIL(rating_share) AS rating_share_whole,
        COALESCE(city, 'OTHER') AS city_clean_2
    FROM filtered
    WHERE country = 'DE'
)
-- Grouped metrics
SELECT
    country,
    SIZE(COLLECT_SET(country)) AS num_countries,
    APPROX_PERCENTILE(rating_share_whole, ARRAY(0.25, 0.5, 0.75)) AS rating_share_whole_quartiles,
    SUM(CASE WHEN country = 'DE' THEN rating_share_whole ELSE 0 END) AS de_rating_share_whole
FROM valid
GROUP BY country
HAVING num_countries < 1
ORDER BY num_countries;
