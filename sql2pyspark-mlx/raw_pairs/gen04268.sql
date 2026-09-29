WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN rating >= 4.5 THEN 'High' WHEN rating >= 2.5 THEN 'Medium' ELSE 'Low' END AS rating_band,
        SUBSTRING(country, 1, 4) AS country_prefix
    FROM suppliers
    WHERE city <> 'New York'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        rating,
        rating_band,
        SUM(rating) OVER w_hist AS running_rating,
        SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating,
        NTILE(3) OVER (PARTITION BY city ORDER BY rating, id) AS rating_bucket
    FROM filtered
    WINDOW w_hist AS (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        rating,
        cumulative_rating,
        rating_bucket,
        ROUND(rating_bucket / SUM(rating_bucket) OVER (PARTITION BY city), 4) AS rating_bucket_share,
        AVG(rating_bucket) OVER w_hist AS rating_bucket_avg_3
    FROM enriched
    WINDOW w_hist AS (ORDER BY cumulative_rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(rating_bucket_avg_3) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating_bucket_avg_3,
        NTILE(3) OVER (PARTITION BY city ORDER BY rating_bucket, id) AS rating_bucket_bucket
    FROM metrics
    WHERE rating <= 2.0
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_rating_bucket_avg_3 >= 43 THEN 'HIGH' ELSE 'LOW' END AS running_rating_bucket_avg_3_tier,
    COUNT(DISTINCT city) AS distinct_cities,
    SUM(running_rating_bucket_avg_3) FILTER (WHERE city = 'Tokyo') AS tokyo_total_running_rating_bucket_avg_3,
    ROUND(SUM(rating_bucket_avg_3) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS rating_bucket_avg_3_per_new_york,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM with_windows
GROUP BY CASE WHEN running_rating_bucket_avg_3 >= 43 THEN 'HIGH' ELSE 'LOW' END
HAVING rating_bucket_avg_3_per_new_york > 0
ORDER BY tokyo_total_running_rating_bucket_avg_3 NULLS FIRST
