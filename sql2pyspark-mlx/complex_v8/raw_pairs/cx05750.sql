WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 10 AND 14
      AND event_type <> 'resolved'
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM scoped_events s
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        actor,
        ROUND(PERCENT_RANK() OVER (ORDER BY last_event_at DESC), 3) AS last_event_at_percent_rank,
        LAST_VALUE(last_event_at) IGNORE NULLS OVER (PARTITION BY actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_event_at,
        FIRST_VALUE(event_at) OVER (ORDER BY last_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_2,
        ROUND(CUME_DIST() OVER (PARTITION BY actor ORDER BY event_at DESC, id), 3) AS event_at_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY last_event_at DESC, id) AS last_event_at_seq,
        FIRST_VALUE(first_event_at) OVER (ORDER BY last_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_first_event_at
    FROM with_counts
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(event_at_cume_dist, last_event_at_seq) AS least_event_at_cume_dist_last_event_at_seq,
        ROUND(event_at_cume_dist * 1.1, 2) AS event_at_cume_dist_adj
    FROM ranked
    WHERE HOUR(last_known_last_event_at) BETWEEN 7 AND 9
),
actor_stats AS (
    -- Median event_at_cume_dist per actor
    SELECT actor, PERCENTILE_APPROX(event_at_cume_dist, 0.5) AS median_event_at_cume_dist
    FROM enriched
    GROUP BY actor
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_event_at_cume_dist,
        ROUND(s.event_at_cume_dist - g.median_event_at_cume_dist, 2) AS event_at_cume_dist_minus_median
    FROM enriched s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.event_at_cume_dist >= g.median_event_at_cume_dist
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.last_event_at_percent_rank,
        s.last_known_last_event_at,
        s.first_event_at_2,
        s.event_at_cume_dist,
        s.last_event_at_seq,
        s.least_event_at_cume_dist_last_event_at_seq,
        s.median_event_at_cume_dist,
        s.event_at_cume_dist_minus_median,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor,
        t.event_at
    FROM compared s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM line_level_events
    WHERE last_event_at_percent_rank > (SELECT MIN(last_event_at_percent_rank) FROM line_level_events)
)
-- Final projection
SELECT
    id,
    ticket_id,
    actor,
    last_event_at_percent_rank,
    last_known_last_event_at,
    last_event_at_seq,
    event_at_cume_dist_minus_median,
    event_id,
    event_type,
    event_at,
    LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
    ROUND(event_at_cume_dist * 1.5, 2) AS event_at_cume_dist_adj
FROM with_activity
ORDER BY event_at DESC NULLS FIRST
