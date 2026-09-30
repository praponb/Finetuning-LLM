WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS rank_in_group
        FROM suppliers
    ) ranked
    WHERE rank_in_group = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id ASC) AS latest_rank
    FROM deduped
    QUALIFY latest_rank = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        country,
        city,
        rating,
        latest_rank,
        CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END is_singapore
    FROM latest
    WHERE city IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC NULLS LAST, id ASC) AS latest_rank_2
    FROM cleaned
    QUALIFY latest_rank_2 = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        country,
        city,
        rating,
        latest_rank,
        is_singapore,
        latest_rank_2,
        LEAST(latest_rank_2, is_singapore) least_latest_rank_2_is_singapore
    FROM ranked
    WHERE city IS NOT NULL
      AND rating < 3.5
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY rating ASC NULLS LAST) rating_dense_rank,
        CUME_DIST() OVER (PARTITION BY country ORDER BY rating ASC) rating_cume_dist,
        LAST_VALUE(rating) IGNORE NULLS OVER (ORDER BY is_singapore ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_rating
    FROM valid
),
deduped_suppliers AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY city, country ORDER BY latest_rank ASC) = 1
)
-- Grouped metrics
SELECT
    CASE WHEN least_latest_rank_2_is_singapore >= 80 THEN 'LARGE' ELSE 'SMALL' END AS least_latest_rank_2_is_singapore_tier,
    CASE WHEN last_known_rating >= 3.5 THEN 'LARGE' ELSE 'SMALL' END AS last_known_rating_tier,
    PERCENTILE_APPROX(last_known_rating, 0.5) AS last_known_rating_p50,
    COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) AS bangkok_count,
    AVG(rating_cume_dist) FILTER (WHERE country = 'JP') AS jp_avg_rating_cume_dist,
    COUNT(*) FILTER (WHERE country = 'SG') AS sg_rows,
    COUNT_IF(city = 'Tokyo') AS tokyo_events,
    COUNT(*) AS event_count
FROM deduped_suppliers
GROUP BY CASE WHEN least_latest_rank_2_is_singapore >= 80 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN last_known_rating >= 3.5 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(*) > 1
ORDER BY least_latest_rank_2_is_singapore_tier DESC NULLS FIRST;
