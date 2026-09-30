WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND actor IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id), 3) AS event_at_percent_rank,
        NTILE(3) OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id) AS event_at_tercile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_all_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC), 3) AS event_at_percent_rank_2,
        LEAD(event_at, 2) OVER (ORDER BY event_at, id) AS next2_event_at,
        ROUND(CUME_DIST() OVER w_all_by_event_at, 3) AS event_at_cume_dist
    FROM scoped_events
    WINDOW w_all_by_event_at AS (ORDER BY event_at, id)
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at_2
    FROM sequenced s
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(event_at_cume_dist - AVG(event_at_cume_dist) OVER (PARTITION BY event_type), 2) AS event_at_cume_dist_vs_event_type_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY actor, DATE_TRUNC('MONTH', next2_event_at) ORDER BY event_at_percent_rank_2 DESC NULLS FIRST), 3) AS event_at_percent_rank_2_cume_dist,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor, DATE_TRUNC('MONTH', next2_event_at) ORDER BY event_at_percent_rank_2 DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LAST_VALUE(minutes_since_prev_event_at) IGNORE NULLS OVER (PARTITION BY actor, DATE_TRUNC('MONTH', next2_event_at) ORDER BY event_at_percent_rank_2 DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_minutes_since_prev_event_at,
        COUNT(*) OVER (PARTITION BY event_type) AS rows_in_event_type
    FROM with_stats
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_percent_rank,
        s.event_at_tercile,
        s.minutes_since_prev_event_at,
        s.event_at_percent_rank_2,
        s.next2_event_at,
        s.event_at_cume_dist,
        s.first_event_at,
        s.first_event_at_2,
        s.event_at_cume_dist_vs_event_type_avg,
        s.event_at_percent_rank_2_cume_dist,
        s.first_known_event_at,
        s.rows_in_event_type,
        t.opened_at AS ticket_opened_at
    FROM sequenced_2 s
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    event_at_percent_rank,
    event_at_tercile,
    minutes_since_prev_event_at,
    event_at_percent_rank_2,
    next2_event_at,
    event_at_cume_dist,
    first_event_at,
    event_at_cume_dist_vs_event_type_avg,
    event_at_percent_rank_2_cume_dist,
    first_known_event_at,
    rows_in_event_type,
    ticket_opened_at,
    LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
    event_at_cume_dist + rows_in_event_type AS event_at_cume_dist_plus_rows_in_event_type
FROM joined
ORDER BY id DESC NULLS FIRST, first_event_at
LIMIT 20
