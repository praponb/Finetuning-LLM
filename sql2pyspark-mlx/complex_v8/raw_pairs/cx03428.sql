WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('DAY', event_at) AS event_at_day,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE actor IN ('agent', 'customer')
      AND event_at < TIMESTAMP '2025-02-18 09:00:00'
),
latest AS (
    -- Keep the earliest row per actor, event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_day,
        event_at_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, event_type ORDER BY event_at_date, id) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
joined_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_date,
        t.priority AS ticket_priority,
        t.closed_at AS ticket_closed_at,
        t.csat AS ticket_csat,
        su.agent_name,
        su.tier AS agent_tier
    FROM latest s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        actor,
        event_at_date,
        ticket_priority,
        ticket_closed_at,
        ticket_csat,
        agent_name,
        ROUND(AVG(ticket_csat) OVER w_ticket_id_ticket_priority, 2) AS ticket_id_ticket_priority_avg_ticket_csat,
        ROUND((UNIX_TIMESTAMP(ticket_closed_at) - UNIX_TIMESTAMP(LAG(ticket_closed_at) OVER (ORDER BY ticket_csat ASC, id))) / 60, 1) AS minutes_since_prev_ticket_closed_at,
        SUM(ticket_csat) OVER (PARTITION BY event_type ORDER BY ticket_closed_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ticket_csat_sum_3,
        ROUND(ticket_csat - AVG(ticket_csat) OVER (PARTITION BY ticket_id, ticket_priority), 2) AS ticket_csat_vs_ticket_id_ticket_priority_avg,
        SUM(ticket_csat) OVER w_ticket_id_ticket_priority AS ticket_id_ticket_priority_sum_ticket_csat,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY ticket_id, ticket_priority)) AS distinct_event_type_in_ticket_id_ticket_priority
    FROM joined_ticket
    WINDOW w_ticket_id_ticket_priority AS (PARTITION BY ticket_id, ticket_priority)
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM with_trends
    WHERE ticket_csat_sum_3 < (SELECT MAX(ticket_csat_sum_3) FROM with_trends)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY ticket_csat, actor) AS rows_in_ticket_csat_actor,
        NTILE(10) OVER w_ticket_id_by_ticket_csat_sum AS ticket_csat_sum_3_decile
    FROM qualified
    WINDOW w_ticket_id_by_ticket_csat_sum AS (PARTITION BY ticket_id ORDER BY ticket_csat_sum_3 DESC NULLS FIRST, id)
),
actor_stats AS (
    -- Max ticket_id_ticket_priority_avg_ticket_csat per actor
    SELECT actor, MAX(ticket_id_ticket_priority_avg_ticket_csat) AS max_ticket_id_ticket_priority_avg_ticket_csat
    FROM metrics
    GROUP BY actor
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ticket_id_ticket_priority_avg_ticket_csat,
        ROUND(s.ticket_id_ticket_priority_avg_ticket_csat - g.max_ticket_id_ticket_priority_avg_ticket_csat, 2) AS ticket_id_ticket_priority_avg_ticket_csat_minus_
    FROM metrics s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.ticket_id_ticket_priority_avg_ticket_csat = g.max_ticket_id_ticket_priority_avg_ticket_csat
)
-- Final projection
SELECT
    id,
    actor,
    ticket_priority,
    ticket_closed_at,
    ticket_csat,
    ticket_id_ticket_priority_avg_ticket_csat,
    minutes_since_prev_ticket_closed_at,
    ticket_csat_vs_ticket_id_ticket_priority_avg,
    distinct_event_type_in_ticket_id_ticket_priority,
    rows_in_ticket_csat_actor,
    ticket_csat_sum_3_decile,
    max_ticket_id_ticket_priority_avg_ticket_csat
FROM compared
ORDER BY max_ticket_id_ticket_priority_avg_ticket_csat DESC, id DESC NULLS FIRST
