WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('agent', 'customer') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group,
        YEAR(event_at) AS event_at_year,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix
    FROM ticket_events
    WHERE event_type IS NOT NULL
      AND ticket_id >= 48
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        actor_group,
        event_at_year,
        LAG(event_at_year, 2) OVER (ORDER BY event_at, id) AS prev2_event_at_year,
        ROUND(AVG(event_at_year) OVER (ORDER BY event_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS event_at_year_avg_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROW_NUMBER() OVER (ORDER BY event_at, id) AS event_at_seq,
        MIN(event_at_year) OVER (PARTITION BY actor_group, event_type) AS actor_group_event_type_min_event_at_year
    FROM cleaned_events
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.agent_id AS ticket_agent_id,
        t.csat AS ticket_csat,
        t.priority AS ticket_priority
    FROM ranked s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(event_at_year_avg_2 - AVG(event_at_year_avg_2) OVER (PARTITION BY actor_group, ticket_priority), 2) AS event_at_year_avg_2_vs_actor_group_ticket_priori,
        ROUND(event_at_seq - AVG(event_at_seq) OVER (PARTITION BY actor_group, ticket_priority), 2) AS event_at_seq_vs_actor_group_ticket_priority_avg,
        LAST_VALUE(event_at_year_avg_2) IGNORE NULLS OVER (PARTITION BY prev2_event_at_year ORDER BY event_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_year_avg_2
    FROM with_details_ticket
)
-- Subtotals with CUBE
SELECT
    event_at_seq,
    prev2_event_at_year,
    actor,
    GROUPING_ID() AS grouping_level,
    SUM(event_at_year_avg_2_vs_actor_group_ticket_priori) FILTER (WHERE ticket_priority = 'P3') AS event_at_year_avg_2_vs_actor_group_ticket_priori_2,
    ROUND(AVG(prev2_event_at_year), 2) AS avg_prev2_event_at_year,
    SUM(actor_group_event_type_min_event_at_year) AS total_actor_group_event_type_min_event_at_year
FROM with_trends
GROUP BY CUBE(event_at_seq, prev2_event_at_year, actor)
ORDER BY event_at_seq NULLS LAST, prev2_event_at_year NULLS LAST, actor NULLS LAST
