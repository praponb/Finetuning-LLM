WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DAYOFWEEK(event_at) AS event_at_dayofweek,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix
    FROM ticket_events
    WHERE ticket_id >= 42
      AND actor = 'agent'
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_dayofweek,
        t.opened_at AS ticket_opened_at,
        t.tags AS ticket_tags,
        t.requester_id AS ticket_requester_id,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM base s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.requester_id <= 23
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_dayofweek,
        ticket_opened_at,
        ticket_tags,
        agent_team,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), MONTH(agent_hired_on)) AS rows_in_event_at_month_agent_hired_on_month,
        MIN(event_at_dayofweek) OVER (PARTITION BY ticket_id) AS ticket_id_min_event_at_dayofweek,
        FIRST_VALUE(agent_hired_on) OVER (PARTITION BY YEAR(event_at), event_at_dayofweek ORDER BY event_at_dayofweek NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_agent_hired_on,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER (PARTITION BY agent_team, MONTH(ticket_opened_at)), 0), 4) AS ticket_requester_id_share_of_agent_team_ticket_o,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), MONTH(agent_hired_on)), 2) AS ticket_requester_id_vs_event_at_month_agent_hire,
        MAX(event_at_dayofweek) OVER (PARTITION BY agent_team, MONTH(ticket_opened_at)) AS agent_team_ticket_opened_at_month_max_event_at_d,
        ROW_NUMBER() OVER w_all_by_ticket_opened_at AS seq_no,
        RANK() OVER (PARTITION BY agent_team, event_type ORDER BY ticket_opened_at DESC) AS ticket_opened_at_rank,
        ROUND(CUME_DIST() OVER (ORDER BY event_at ASC), 3) AS event_at_cume_dist,
        ROUND(AVG(event_at_dayofweek) OVER (PARTITION BY YEAR(event_at), event_at_dayofweek ORDER BY event_at_dayofweek NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS event_at_dayofweek_avg_4,
        ROUND(AVG(ticket_requester_id) OVER (ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ticket_requester_id_avg_2_prior,
        LAST_VALUE(ticket_opened_at) IGNORE NULLS OVER (PARTITION BY agent_team, event_type ORDER BY ticket_opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_opened_at
    FROM enriched_ticket
    WINDOW w_all_by_ticket_opened_at AS (ORDER BY ticket_opened_at DESC, id)
),
first_rows AS (
    -- Top 2 per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_dayofweek,
        ticket_opened_at,
        ticket_tags,
        agent_team,
        rows_in_event_at_month_agent_hired_on_month,
        ticket_id_min_event_at_dayofweek,
        first_agent_hired_on,
        ticket_requester_id_share_of_agent_team_ticket_o,
        ticket_requester_id_vs_event_at_month_agent_hire,
        agent_team_ticket_opened_at_month_max_event_at_d,
        ticket_opened_at_rank,
        event_at_cume_dist,
        event_at_dayofweek_avg_4,
        ticket_requester_id_avg_2_prior,
        last_known_ticket_opened_at
    FROM metrics
    WHERE seq_no <= 2
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_at_dayofweek,
        ticket_opened_at,
        agent_team,
        ticket_id_min_event_at_dayofweek,
        ticket_requester_id_share_of_agent_team_ticket_o,
        agent_team_ticket_opened_at_month_max_event_at_d,
        event_at_cume_dist,
        event_at_dayofweek_avg_4,
        last_known_ticket_opened_at,
        COUNT(*) OVER w_ticket_opened_at_rank AS rows_in_ticket_opened_at_rank,
        FIRST_VALUE(ticket_opened_at) OVER (PARTITION BY event_type, ticket_opened_at_rank ORDER BY ticket_requester_id_share_of_agent_team_ticket_o DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_opened_at,
        LEAD(event_at_dayofweek, 3, 0) OVER w_event_type_ticket_opened_at_2 AS next3_event_at_dayofweek,
        NTILE(10) OVER (PARTITION BY event_type, ticket_opened_at_rank ORDER BY ticket_requester_id_share_of_agent_team_ticket_o DESC, id) AS ticket_requester_id_share_of_agent_team_ticket_o_2
    FROM first_rows
    WINDOW w_ticket_opened_at_rank AS (PARTITION BY ticket_opened_at_rank), w_event_type_ticket_opened_at_2 AS (PARTITION BY event_type, ticket_opened_at_rank ORDER BY ticket_requester_id_share_of_agent_team_ticket_o DESC, id)
),
per_group AS (
    -- Group-level totals
    SELECT
        agent_team,
        next3_event_at_dayofweek,
        YEAR(first_ticket_opened_at) AS first_ticket_opened_at_year,
        ROUND(AVG(agent_team_ticket_opened_at_month_max_event_at_d), 2) AS avg_agent_team_ticket_opened_at_month_max_event_,
        ARRAY_SORT(COLLECT_SET(agent_team)) AS agent_team_list,
        COUNT(*) FILTER (WHERE event_type <> 'created') AS n_event_type_created,
        ROUND(100 * COUNT(CASE WHEN agent_team = 'Billing' THEN 1 END) / COUNT(*), 1) AS pct_agent_team_billing
    FROM ranked
    GROUP BY agent_team, next3_event_at_dayofweek, YEAR(first_ticket_opened_at)
)
-- Final projection
SELECT
    agent_team,
    next3_event_at_dayofweek,
    first_ticket_opened_at_year,
    agent_team_list,
    pct_agent_team_billing
FROM per_group
ORDER BY agent_team DESC NULLS FIRST
