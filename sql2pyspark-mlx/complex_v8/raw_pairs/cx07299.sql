WITH prepared_events AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
    WHERE actor IN ('customer', 'agent')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        DENSE_RANK() OVER w_all_by_event_at AS event_at_dense_rank,
        LAST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROUND(CUME_DIST() OVER (ORDER BY event_at DESC, id), 3) AS event_at_cume_dist
    FROM prepared_events
    WINDOW w_all_by_event_at AS (ORDER BY event_at DESC, id)
),
ticket_id_stats AS (
    -- Max event_at_cume_dist per ticket_id
    SELECT ticket_id, MAX(event_at_cume_dist) AS max_event_at_cume_dist
    FROM ranked
    GROUP BY ticket_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_event_at_cume_dist,
        ROUND(s.event_at_cume_dist - g.max_event_at_cume_dist, 2) AS event_at_cume_dist_minus_max
    FROM ranked s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.event_at_cume_dist = g.max_event_at_cume_dist
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        ROUND(event_at_cume_dist * 1.1, 2) AS event_at_cume_dist_adj
    FROM compared
    WHERE event_at_cume_dist_minus_max BETWEEN -0.4 AND 0.9
),
totals AS (
    -- Group-level totals
    SELECT
        event_at_dense_rank,
        MAX(event_at_cume_dist) AS max_event_at_cume_dist_2,
        ROUND(AVG(event_at_cume_dist_adj), 2) AS avg_event_at_cume_dist_adj
    FROM tagged
    GROUP BY 1
    HAVING AVG(max_event_at_cume_dist) > 0.2
)
-- Final projection
SELECT
    event_at_dense_rank,
    max_event_at_cume_dist_2,
    ABS(event_at_dense_rank - 4) AS event_at_dense_rank_distance,
    ABS(event_at_dense_rank - 6) AS event_at_dense_rank_distance_2
FROM totals
ORDER BY max_event_at_cume_dist_2 NULLS LAST, event_at_dense_rank_distance
