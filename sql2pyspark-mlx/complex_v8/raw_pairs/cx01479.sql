WITH valid AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity
    FROM warehouses
    WHERE capacity BETWEEN 2614 AND 11768
      AND city NOT IN ('Singapore', 'Kuala Lumpur')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(capacity) OVER (ORDER BY capacity NULLS LAST, id) AS prev_capacity,
        ROUND(CUME_DIST() OVER (ORDER BY capacity NULLS LAST), 3) AS capacity_cume_dist,
        DENSE_RANK() OVER (ORDER BY capacity NULLS LAST) AS capacity_dense_rank,
        LEAD(capacity) OVER (ORDER BY capacity NULLS LAST, id) AS next_capacity
    FROM valid
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_windows
    WHERE capacity_cume_dist > (SELECT AVG(capacity_cume_dist) FROM with_windows)
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(city, 'ALL') AS city,
    capacity_dense_rank,
    GROUPING_ID() AS grouping_level,
    SUM(CASE WHEN city NOT IN ('Kuala Lumpur', 'Bangkok') THEN prev_capacity ELSE 0 END) AS prev_capacity_city_not_in_kuala_lumpur_ban,
    ROUND(SUM(capacity_dense_rank) / NULLIF(SUM(prev_capacity), 0), 3) AS capacity_dense_rank_to_prev_capacity_ratio,
    SUM(CASE WHEN LOWER(wh_code) LIKE '%bk%' THEN capacity_cume_dist ELSE 0 END) AS capacity_cume_dist_lower_wh_code_like_bk
FROM qualified
GROUP BY GROUPING SETS ((city, capacity_dense_rank), (city), ())
