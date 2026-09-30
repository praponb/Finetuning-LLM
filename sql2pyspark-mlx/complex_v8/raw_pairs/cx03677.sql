WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        ROUND(PERCENT_RANK() OVER w_ticket_id_by_event_at_ties, 3) AS event_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        ROUND(CUME_DIST() OVER w_ticket_id_by_event_at, 3) AS event_at_cume_dist_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at
    FROM valid
    WINDOW w_ticket_id_by_event_at_ties AS (PARTITION BY ticket_id ORDER BY event_at DESC), w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at DESC, id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        minutes_since_prev_event_at,
        LAST_VALUE(event_at_cume_dist_2) IGNORE NULLS OVER (ORDER BY minutes_since_prev_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_cume_dist_2,
        SUM(event_at_cume_dist_2) OVER (ORDER BY minutes_since_prev_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_at_cume_dist_2,
        FIRST_VALUE(minutes_since_prev_event_at) OVER (ORDER BY minutes_since_prev_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_minutes_since_prev_event_at,
        ROUND(AVG(event_at_cume_dist_2) OVER (ORDER BY event_at_percent_rank NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS event_at_cume_dist_2_avg_6_prior,
        SUM(event_at_percent_rank) OVER (ORDER BY minutes_since_prev_event_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS event_at_percent_rank_sum_3_prior
    FROM with_trends
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(minutes_since_prev_event_at) IGNORE NULLS OVER (ORDER BY minutes_since_prev_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_minutes_since_prev_event_at,
        ROUND(CUME_DIST() OVER (ORDER BY last_known_event_at_cume_dist_2 DESC), 3) AS last_known_event_at_cume_dist_2_cume_dist,
        FIRST_VALUE(minutes_since_prev_event_at) IGNORE NULLS OVER (ORDER BY minutes_since_prev_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_minutes_since_prev_event_at,
        NTH_VALUE(minutes_since_prev_event_at, 2) OVER (ORDER BY minutes_since_prev_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_minutes_since_prev_event_at
    FROM windowed
)
-- Final projection
SELECT
    id,
    minutes_since_prev_event_at,
    last_known_event_at_cume_dist_2,
    running_event_at_cume_dist_2,
    first_minutes_since_prev_event_at,
    event_at_percent_rank_sum_3_prior,
    last_known_minutes_since_prev_event_at,
    last_known_event_at_cume_dist_2_cume_dist,
    first_known_minutes_since_prev_event_at
FROM with_windows
ORDER BY running_event_at_cume_dist_2
