WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        YEAR(event_at) AS event_at_year
    FROM ticket_events
    WHERE (HOUR(event_at) BETWEEN 7 AND 9 OR actor IN ('customer', 'agent'))
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_year,
        t.csat AS ticket_csat,
        su.team AS agent_team
    FROM base s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND ARRAY_CONTAINS(t.tags, 'upgrade')
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(ticket_csat) OVER (PARTITION BY event_type, event_at_year ORDER BY event_at_year DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ticket_csat_sum_4,
        SIZE(COLLECT_SET(event_type) OVER w_actor_ticket_id) AS distinct_event_type_in_actor_ticket_id,
        COUNT(*) OVER (PARTITION BY actor, ticket_id) AS rows_in_actor_ticket_id,
        LAST_VALUE(ticket_csat) OVER (PARTITION BY event_at_year ORDER BY event_at_year NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_csat,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY event_type, event_at_year ORDER BY event_at_year DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS ticket_csat_avg_6_prior,
        LAST_VALUE(ticket_csat) IGNORE NULLS OVER (PARTITION BY event_type, event_at_year ORDER BY event_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_csat
    FROM with_details
    WINDOW w_actor_ticket_id AS (PARTITION BY actor, ticket_id)
),
per_group AS (
    -- Group-level totals
    SELECT
        distinct_event_type_in_actor_ticket_id,
        agent_team,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        COUNT(*) AS row_count,
        MIN(ticket_csat) AS min_ticket_csat,
        COUNT(DISTINCT agent_team) AS distinct_agent_teams,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        SUM(last_known_ticket_csat) FILTER (WHERE event_type NOT IN ('escalated', 'resolved')) AS last_known_ticket_csat_event_type_not_in_escalat
    FROM with_windows
    GROUP BY distinct_event_type_in_actor_ticket_id, agent_team, DATE_TRUNC('MONTH', event_at)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_group
    WHERE last_known_ticket_csat_event_type_not_in_escalat > (SELECT AVG(last_known_ticket_csat_event_type_not_in_escalat) FROM per_group)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        distinct_event_type_in_actor_ticket_id,
        agent_team,
        event_at_month,
        row_count,
        min_ticket_csat,
        distinct_agent_teams,
        distinct_event_types,
        ROW_NUMBER() OVER (PARTITION BY distinct_event_types, min_ticket_csat ORDER BY row_count, distinct_event_type_in_actor_ticket_id, agent_team, event_at_month) AS rn,
        RANK() OVER (PARTITION BY distinct_event_types, min_ticket_csat ORDER BY row_count) AS row_count_rank,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY distinct_event_type_in_actor_ticket_id)) AS distinct_agent_team_in_distinct_event_type_in_ac,
        ROUND(AVG(distinct_event_types) OVER (PARTITION BY distinct_event_type_in_actor_ticket_id), 2) AS distinct_event_type_in_actor_ticket_id_avg_disti
    FROM filtered
)
-- Final projection
SELECT
    distinct_event_type_in_actor_ticket_id,
    agent_team,
    event_at_month,
    row_count,
    distinct_event_types,
    row_count_rank,
    distinct_agent_team_in_distinct_event_type_in_ac,
    CONCAT(agent_team, '#', CAST(row_count AS STRING)) AS agent_team_ref,
    CASE WHEN agent_team IN ('Billing', 'VIP') THEN 'group_a' WHEN agent_team = 'Tech' THEN 'group_b' ELSE 'other' END AS agent_team_group
FROM windowed
