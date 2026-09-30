WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 8 AND 10
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor
    FROM valid_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.actor = 'customer'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(event_at) OVER (ORDER BY event_at, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_event_type ORDER BY event_at DESC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at
    FROM exploded_detail
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id, event_id), 3) AS event_at_percent_rank,
        LAG(first_known_event_at, 3) OVER (ORDER BY event_at, id, event_id) AS prev3_first_known_event_at
    FROM ranked
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        event_actor,
        actor,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT event_event_type) AS distinct_event_event_types,
        SUM(CASE WHEN event_actor IN ('agent', 'system') THEN event_at_percent_rank ELSE 0 END) AS event_at_percent_rank_event_actor_in_agent_syste,
        MIN(event_at_percent_rank) AS min_event_at_percent_rank,
        SUM(event_at_percent_rank) AS total_event_at_percent_rank
    FROM metrics
    GROUP BY DATE_TRUNC('MONTH', event_at), event_actor, actor
),
scored AS (
    -- Add calculated columns
    SELECT
        event_at_month,
        event_actor,
        actor,
        distinct_ticket_ids,
        distinct_event_event_types,
        event_at_percent_rank_event_actor_in_agent_syste,
        min_event_at_percent_rank,
        SUBSTRING(TRIM(event_actor), 1, 3) AS event_actor_prefix,
        DAYOFWEEK(event_at_month) AS event_at_month_dayofweek,
        CONCAT(event_actor, '#', CAST(n_rows AS STRING)) AS event_actor_ref
    FROM totals
    WHERE MONTH(event_at_month) IN (2, 3)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM scored
    WHERE distinct_ticket_ids > (SELECT AVG(distinct_ticket_ids) FROM scored)
)
-- Final projection
SELECT
    event_at_month,
    actor,
    distinct_ticket_ids,
    distinct_event_event_types,
    event_at_percent_rank_event_actor_in_agent_syste,
    min_event_at_percent_rank,
    event_actor_prefix,
    event_at_month_dayofweek,
    CONCAT(event_actor_ref, '#', CAST(event_at_month_dayofweek AS STRING)) AS event_actor_ref_ref
FROM screened
ORDER BY event_actor_prefix NULLS LAST, distinct_event_event_types
