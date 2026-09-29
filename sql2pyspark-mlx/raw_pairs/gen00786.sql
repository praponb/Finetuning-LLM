WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_running AS event_seq,
        LEAD(rating, 2) OVER (ORDER BY rating ASC, id ASC) AS next_rating,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country, city), 4) AS rating_share_2
    FROM suppliers
    WINDOW w_running AS (PARTITION BY country ORDER BY rating DESC, id ASC)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        rating,
        event_seq,
        next_rating,
        rating_share,
        MAX(rating_share) OVER w_ordered AS running_max_rating_share,
        SUM(rating_share) OVER w_ordered AS running_sum_rating_share,
        MIN(rating_share) OVER w_ordered AS running_min_rating_share,
        AVG(rating_share) OVER w_ordered AS running_avg_rating_share
    FROM scored
    WINDOW w_ordered AS (PARTITION BY city, country ORDER BY rating_share ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, city
    FROM with_windows
    EXCEPT DISTINCT
    SELECT id, city
    FROM suppliers
    WHERE city NOT IN ('New York', 'Berlin')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id AND s.city = k.city
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        rating,
        event_seq,
        next_rating,
        rating_share,
        running_max_rating_share,
        running_min_rating_share,
        running_avg_rating_share,
        ROUND(running_avg_rating_share / rating, 2) AS running_avg_rating_share_per_rating,
        COALESCE(city, 'NONE') AS resolved_city
    FROM kept_rows
    WHERE running_avg_rating_share >= 0.73
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.brand = 'Initech'
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(rating) OVER (PARTITION BY city) AS city_max_rating,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY rating_share DESC NULLS FIRST) AS rating_share_percent_rank
    FROM kept
)
-- Grouped metrics
SELECT
    city,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(rating), 2) AS avg_rating,
    COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) AS bangkok_count,
    SUM(CASE WHEN city = 'Bangkok' THEN rating_share_percent_rank ELSE 0 END) AS bangkok_rating_share_percent_rank,
    COUNT(*) FILTER (WHERE city = 'Berlin') AS berlin_rows
FROM metrics
GROUP BY city
HAVING COUNT(DISTINCT id) > 1
ORDER BY bangkok_count ASC NULLS LAST
