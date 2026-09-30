WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DAYOFWEEK(event_at) AS event_at_dayofweek,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.closed_at AS ticket_closed_at,
        t.opened_at AS ticket_opened_at,
        t.requester_id AS ticket_requester_id,
        su.team AS agent_team
    FROM filtered_events s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(event_at_dayofweek) OVER (ORDER BY event_at_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_event_at_dayofweek_so_far,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY actor, YEAR(ticket_closed_at))) AS distinct_agent_team_in_actor_ticket_closed_at_ye,
        ROUND(AVG(days_since_event_at) OVER (PARTITION BY actor, YEAR(ticket_closed_at)), 2) AS actor_ticket_closed_at_year_avg_days_since_event,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY days_since_event_at DESC), 3) AS days_since_event_at_percent_rank,
        MAX(days_since_event_at) OVER (ORDER BY days_since_event_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS days_since_event_at_max_7,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY actor, YEAR(ticket_closed_at))) AS distinct_event_type_in_actor_ticket_closed_at_ye,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        COUNT(*) OVER w_actor_ticket_closed_at_year AS rows_in_actor_ticket_closed_at_year,
        ROUND(event_at_dayofweek - AVG(event_at_dayofweek) OVER (PARTITION BY actor, YEAR(ticket_closed_at)), 2) AS event_at_dayofweek_vs_actor_ticket_closed_at_yea,
        ROUND(AVG(ticket_requester_id) OVER w_actor_ticket_closed_at_year, 2) AS actor_ticket_closed_at_year_avg_ticket_requester,
        LAG(event_at) OVER (ORDER BY days_since_event_at DESC, id) AS prev_event_at
    FROM enriched_ticket
    WINDOW w_actor_ticket_closed_at_year AS (PARTITION BY actor, YEAR(ticket_closed_at))
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(agent_team), 1, 4) AS agent_team_prefix,
        LENGTH(TRIM(agent_team)) AS agent_team_len
    FROM ranked
    WHERE actor IN ('customer', 'agent')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(days_since_event_at) OVER w_distinct_event_type_in_actor, 2) AS distinct_event_type_in_actor_ticket_closed_at_ye_2,
        ROUND(AVG(days_since_event_at) OVER (PARTITION BY MONTH(last_event_at), MONTH(prev_event_at) ORDER BY days_since_event_at_percent_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS days_since_event_at_avg_3_prior,
        MIN(days_since_event_at_max_7) OVER w_distinct_event_type_in_actor AS distinct_event_type_in_actor_ticket_closed_at_ye_3,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY distinct_event_type_in_actor_ticket_closed_at_ye)) AS distinct_ticket_id_in_distinct_event_type_in_act,
        LEAD(event_at) OVER (ORDER BY event_at DESC, id) AS next_event_at
    FROM derived
    WINDOW w_distinct_event_type_in_actor AS (PARTITION BY distinct_event_type_in_actor_ticket_closed_at_ye)
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    actor,
    event_at_dayofweek,
    days_since_event_at,
    ticket_closed_at,
    ticket_requester_id,
    agent_team,
    max_event_at_dayofweek_so_far,
    distinct_agent_team_in_actor_ticket_closed_at_ye,
    actor_ticket_closed_at_year_avg_days_since_event,
    days_since_event_at_percent_rank,
    last_known_event_at,
    rows_in_actor_ticket_closed_at_year,
    event_at_dayofweek_vs_actor_ticket_closed_at_yea,
    actor_ticket_closed_at_year_avg_ticket_requester,
    prev_event_at,
    agent_team_prefix,
    distinct_event_type_in_actor_ticket_closed_at_ye_2,
    distinct_event_type_in_actor_ticket_closed_at_ye_3,
    distinct_ticket_id_in_distinct_event_type_in_act,
    next_event_at,
    COALESCE(ticket_requester_id, 0) AS ticket_requester_id_filled,
    agent_team_len - days_since_event_at AS agent_team_len_minus_days_since_event_at
FROM metrics
ORDER BY prev_event_at DESC NULLS FIRST, event_at DESC
