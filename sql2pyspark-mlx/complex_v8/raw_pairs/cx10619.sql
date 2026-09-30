WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (HOUR(event_at) BETWEEN 10 AND 13 OR ticket_id <= 27)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        ROW_NUMBER() OVER w_event_type_by_event_at AS event_at_seq,
        RANK() OVER (ORDER BY event_at ASC) AS event_at_rank,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROUND(CUME_DIST() OVER (ORDER BY event_at ASC), 3) AS event_at_cume_dist,
        ROUND(CUME_DIST() OVER (ORDER BY event_at ASC, id), 3) AS event_at_cume_dist_2
    FROM recent_events
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at ASC, id)
),
event_type_stats AS (
    -- Avg event_at_rank per event_type
    SELECT event_type, AVG(event_at_rank) AS avg_event_at_rank
    FROM sequenced
    GROUP BY event_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_event_at_rank,
        ROUND(s.event_at_rank - g.avg_event_at_rank, 2) AS event_at_rank_minus_avg
    FROM sequenced s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.event_at_rank > g.avg_event_at_rank
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT COUNT(*) FILTER (WHERE t.event_type IS NOT NULL) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_event_type_count
    FROM vs_group s
),
actor_stats AS (
    -- Max avg_event_at_rank per actor
    SELECT actor, MAX(avg_event_at_rank) AS max_avg_event_at_rank
    FROM profiled
    GROUP BY actor
),
vs_group_2 AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_avg_event_at_rank,
        ROUND(s.avg_event_at_rank - g.max_avg_event_at_rank, 2) AS avg_event_at_rank_minus_max
    FROM profiled s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.avg_event_at_rank = g.max_avg_event_at_rank
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    event_at_seq,
    event_at_rank,
    last_known_event_at,
    event_at_cume_dist,
    event_at_cume_dist_2,
    avg_event_at_rank,
    event_at_rank_minus_avg,
    last_event_at,
    first_event_at,
    event_event_type_count,
    max_avg_event_at_rank,
    avg_event_at_rank_minus_max
FROM vs_group_2
WHERE HOUR(last_known_event_at) BETWEEN 8 AND 12
