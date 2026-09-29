WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE MONTH(event_at) IN (1, 2)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        days_since_event_at,
        COUNT(*) OVER (PARTITION BY actor, event_type) AS rows_in_actor_event_type,
        SUM(days_since_event_at) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_event_at,
        ROUND(days_since_event_at - AVG(days_since_event_at) OVER (PARTITION BY event_type, DATE_TRUNC('MONTH', event_at)), 2) AS days_since_event_at_vs_event_type_event_at_month,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(event_at), actor ORDER BY days_since_event_at ASC), 3) AS days_since_event_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_at, id), 3) AS event_at_cume_dist,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY actor, event_type)) AS distinct_ticket_id_in_actor_event_type,
        DENSE_RANK() OVER (ORDER BY event_at DESC NULLS FIRST, id) AS event_at_dense_rank,
        ROW_NUMBER() OVER w_event_at_year_actor_by_days_2 AS days_since_event_at_seq,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER w_actor_event_type, 0), 4) AS days_since_event_at_share_of_actor_event_type
    FROM recent_events
    WINDOW w_event_at_year_actor_by_days_2 AS (PARTITION BY YEAR(event_at), actor ORDER BY days_since_event_at ASC, id), w_actor_event_type AS (PARTITION BY actor, event_type)
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM with_windows
    WHERE event_at_dense_rank <= 2
),
with_windows_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_type,
        actor,
        days_since_event_at,
        rows_in_actor_event_type,
        days_since_event_at_vs_event_type_event_at_month,
        days_since_event_at_percent_rank,
        event_at_cume_dist,
        distinct_ticket_id_in_actor_event_type,
        event_at_dense_rank,
        SUM(days_since_event_at_seq) OVER (PARTITION BY event_type, ticket_id ORDER BY days_since_event_at_percent_rank DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS days_since_event_at_seq_sum_2,
        SUM(event_at_dense_rank) OVER (PARTITION BY ticket_id, distinct_ticket_id_in_actor_event_type) AS ticket_id_distinct_ticket_id_in_actor_event_type,
        LAST_VALUE(event_at_cume_dist) IGNORE NULLS OVER (PARTITION BY actor ORDER BY days_since_event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_cume_dist
    FROM first_rows
)
SELECT
    id,
    event_type,
    days_since_event_at,
    days_since_event_at_vs_event_type_event_at_month,
    days_since_event_at_percent_rank,
    days_since_event_at_seq_sum_2,
    ticket_id_distinct_ticket_id_in_actor_event_type,
    last_known_event_at_cume_dist
FROM with_windows_2
WHERE ticket_id_distinct_ticket_id_in_actor_event_type >= 31
ORDER BY days_since_event_at DESC, ticket_id_distinct_ticket_id_in_actor_event_type
