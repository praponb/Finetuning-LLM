WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN rating >= 3.5 THEN 'High' WHEN rating >= 2.3 THEN 'Medium' ELSE 'Low' END AS rating_level
    FROM suppliers
    WHERE supplier_name LIKE 'P%'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        country,
        city,
        rating,
        AVG(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rating_avg_5,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC NULLS FIRST) AS rating_dense_rank,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share
    FROM base
    WHERE rating >= 2.4
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        rating,
        rating_avg_5,
        rating_dense_rank,
        ROUND(rating_avg_5 * 0.85, 2) AS rating_avg_5_adjusted,
        COALESCE(city, 'OTHER') AS city_filled
    FROM enriched
    WHERE country <> 'UK'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(rating) OVER w_grp AS cumulative_rating,
        NTILE(5) OVER (PARTITION BY city ORDER BY rating_dense_rank, id) AS rating_dense_rank_bucket,
        AVG(rating_dense_rank) OVER (PARTITION BY city) AS city_avg_rating_dense_rank
    FROM filtered
    WINDOW w_grp AS (PARTITION BY city ORDER BY rating_avg_5_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    CASE WHEN rating >= 3.7 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
    CASE WHEN rating_dense_rank_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END AS rating_dense_rank_bucket_tier,
    city,
    COUNT_IF(city = 'Bangkok') AS bangkok_events,
    COUNT_IF(city = 'Chiang Mai') AS chiang_mai_events,
    COUNT_IF(city = 'Bangkok') AS bangkok_events_2
FROM with_windows
GROUP BY CASE WHEN rating >= 3.7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN rating_dense_rank_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END, city
ORDER BY rating_tier;
