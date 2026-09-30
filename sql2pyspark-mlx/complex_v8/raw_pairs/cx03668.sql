WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE event_type <> 'reopened'
      AND ticket_id IS NOT NULL
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT COUNT(CASE WHEN t.actor NOT IN ('system', 'customer') THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_actor_count,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at_2
    FROM recent s
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(first_event_at) - UNIX_TIMESTAMP(LAG(first_event_at) OVER w_actor_event_actor_count_by_f)) / 60, 1) AS minutes_since_prev_first_event_at,
        MAX(event_actor_count) OVER (PARTITION BY ticket_id) AS ticket_id_max_event_actor_count,
        ROUND(event_actor_count - AVG(event_actor_count) OVER w_ticket_id, 2) AS event_actor_count_vs_ticket_id_avg
    FROM with_counts
    WINDOW w_actor_event_actor_count_by_f AS (PARTITION BY actor, event_actor_count ORDER BY first_event_at_2 NULLS LAST, id), w_ticket_id AS (PARTITION BY ticket_id)
)
-- Subtotals with GROUPING SETS
SELECT
    event_actor_count,
    actor,
    MIN(ticket_id_max_event_actor_count) AS min_ticket_id_max_event_actor_count,
    ROUND(STDDEV(ticket_id_max_event_actor_count), 2) AS stddev_ticket_id_max_event_actor_count,
    SUM(event_actor_count_vs_ticket_id_avg) AS total_event_actor_count_vs_ticket_id_avg
FROM metrics
GROUP BY GROUPING SETS ((event_actor_count, actor), (event_actor_count))
ORDER BY event_actor_count NULLS LAST, actor NULLS LAST
