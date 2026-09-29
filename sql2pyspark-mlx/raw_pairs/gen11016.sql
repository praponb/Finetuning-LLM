WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN page = '/checkout' THEN 1 ELSE 0 END is_checkout
    FROM web_events
    WHERE event_time >= DATE '2023-09-24'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT page
    FROM base
    EXCEPT DISTINCT
    SELECT page
    FROM web_events
    WHERE event_type NOT IN ('search', 'purchase')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.page = k.page
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        is_checkout,
        LOWER(page) page_lower,
        COALESCE(page, 'NONE') resolved_page
    FROM reconciled
    WHERE page IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        event_type,
        page,
        page_lower,
        CUME_DIST() OVER (PARTITION BY session_id, event_type ORDER BY is_checkout) is_checkout_cume_dist
    FROM valid
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN is_checkout_cume_dist >= 0.84 THEN 'HIGH' ELSE 'LOW' END AS is_checkout_cume_dist_tier,
        SORT_ARRAY(COLLECT_SET(page)) AS page_list,
        COUNT(*) AS num_rows,
        ROUND(SUM(is_checkout_cume_dist) / NULLIF(COUNT(CASE WHEN page = '/search' THEN 1 END), 0), 2) AS is_checkout_cume_dist_per_search,
        PERCENTILE_APPROX(is_checkout_cume_dist, 0.9) AS is_checkout_cume_dist_p90,
        AVG(is_checkout_cume_dist) FILTER (WHERE page = '/home') AS home_avg_is_checkout_cume_dist
    FROM with_windows
    GROUP BY CASE WHEN is_checkout_cume_dist >= 0.84 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    is_checkout_cume_dist_tier,
    page_list,
    num_rows,
    is_checkout_cume_dist_per_search,
    is_checkout_cume_dist_p90
FROM daily
ORDER BY is_checkout_cume_dist_p90 NULLS FIRST, is_checkout_cume_dist_tier DESC
LIMIT 10
