WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', actor, event_type) AS actor_event_type_label,
        DAYOFWEEK(event_at) AS event_at_dayofweek,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE event_type <> 'replied'
      AND actor NOT IN ('agent', 'system')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        actor_event_type_label,
        event_at_dayofweek,
        COUNT(*) OVER w_event_type AS rows_in_event_type,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type, event_at_dayofweek ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        LEAD(event_at) OVER w_all_by_event_at_dayofweek AS next_event_at,
        RANK() OVER (ORDER BY event_at_dayofweek DESC, id) AS event_at_dayofweek_rank,
        SUM(event_at_dayofweek) OVER (PARTITION BY event_type) AS event_type_sum_event_at_dayofweek
    FROM prepared
    WINDOW w_event_type AS (PARTITION BY event_type), w_all_by_event_at_dayofweek AS (ORDER BY event_at_dayofweek DESC, id)
),
top_per_group AS (
    -- Top 2 per group
    SELECT *
    FROM with_trends
    WHERE event_at_dayofweek_rank <= 2
      AND event_at >= TIMESTAMP '2025-02-06 12:00:00'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        ROUND((UNIX_TIMESTAMP(next_event_at) - UNIX_TIMESTAMP(event_at)) / 60, 1) AS minutes_event_at_to_next_event_at
    FROM top_per_group
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(days_since_event_at) OVER w_event_at_dayofweek_rank, 2) AS event_at_dayofweek_rank_avg_days_since_event_at,
        ROUND(AVG(rows_in_event_type) OVER (PARTITION BY event_at_dayofweek_rank), 2) AS event_at_dayofweek_rank_avg_rows_in_event_type,
        MIN(event_at_percent_rank) OVER w_event_at_dayofweek_rank AS event_at_dayofweek_rank_min_event_at_percent_ran,
        ROUND(CUME_DIST() OVER (PARTITION BY actor, event_at_dayofweek ORDER BY event_at_dayofweek_rank), 3) AS event_at_dayofweek_rank_cume_dist,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY event_at_dayofweek_rank)) AS distinct_ticket_id_in_event_at_dayofweek_rank,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_at_dayofweek_rank)) AS distinct_actor_in_event_at_dayofweek_rank
    FROM labeled
    WINDOW w_event_at_dayofweek_rank AS (PARTITION BY event_at_dayofweek_rank)
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.actor_event_type_label,
        s.rows_in_event_type,
        s.next_event_at,
        s.event_at_dayofweek_rank,
        s.minutes_event_at_to_next_event_at,
        s.event_at_dayofweek_rank_min_event_at_percent_ran,
        s.event_at_dayofweek_rank_cume_dist,
        s.distinct_actor_in_event_at_dayofweek_rank,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM with_windows s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.actor NOT IN ('system', 'agent')
),
labeled_2 AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', next_event_at) AS days_since_next_event_at,
        ABS(event_at_dayofweek_rank - 5) AS event_at_dayofweek_rank_distance
    FROM with_children
    WHERE event_actor <> 'customer'
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    actor_event_type_label,
    rows_in_event_type,
    next_event_at,
    event_at_dayofweek_rank,
    event_at_dayofweek_rank_cume_dist,
    distinct_actor_in_event_at_dayofweek_rank,
    event_id,
    days_since_next_event_at,
    event_at_dayofweek_rank_distance
FROM labeled_2
ORDER BY days_since_next_event_at NULLS LAST
