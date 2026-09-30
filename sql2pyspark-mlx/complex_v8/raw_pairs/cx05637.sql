WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date,
        event_at + INTERVAL 15 MINUTES AS event_at_plus_15m
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.event_at_date,
        s.event_at_plus_15m,
        t.csat AS ticket_csat,
        t.requester_id AS ticket_requester_id,
        t.channel AS ticket_channel,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM scoped_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.requester_id > 3
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at_date,
        event_at_plus_15m,
        ticket_csat,
        ticket_requester_id,
        ticket_channel,
        agent_tier,
        agent_team,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at_date))) AS distinct_ticket_id_in_event_at_date_month,
        SIZE(COLLECT_SET(agent_team) OVER w_event_at_date_month) AS distinct_agent_team_in_event_at_date_month,
        ROUND(AVG(ticket_requester_id) OVER (PARTITION BY ticket_csat ORDER BY ticket_csat ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS ticket_requester_id_avg_6_prior,
        NTILE(4) OVER (PARTITION BY actor, ticket_csat ORDER BY ticket_requester_id DESC, id) AS ticket_requester_id_quartile,
        ROW_NUMBER() OVER (PARTITION BY ticket_csat ORDER BY ticket_csat ASC, id) AS seq_no,
        FIRST_VALUE(event_at_date) IGNORE NULLS OVER (PARTITION BY ticket_channel, DATE_TRUNC('MONTH', event_at_date) ORDER BY event_at_plus_15m, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_date,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at_date)), 0), 4) AS ticket_csat_share_of_event_at_date_month,
        LAG(ticket_requester_id) OVER w_ticket_csat_by_ticket_csat AS prev_ticket_requester_id,
        SIZE(COLLECT_SET(agent_tier) OVER w_event_at_date_month) AS distinct_agent_tier_in_event_at_date_month,
        FIRST_VALUE(ticket_requester_id) IGNORE NULLS OVER (PARTITION BY actor, ticket_csat ORDER BY ticket_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_requester_id,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_channel, DATE_TRUNC('MONTH', event_at_date) ORDER BY event_at_plus_15m, id), 3) AS event_at_plus_15m_cume_dist,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at_date)), 2) AS ticket_requester_id_vs_event_at_date_month_avg,
        RANK() OVER w_ticket_csat_by_ticket_csat AS ticket_csat_rank
    FROM combined_details_ticket
    WINDOW w_event_at_date_month AS (PARTITION BY DATE_TRUNC('MONTH', event_at_date)), w_ticket_csat_by_ticket_csat AS (PARTITION BY ticket_csat ORDER BY ticket_csat ASC, id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at_plus_15m,
        ticket_csat,
        ticket_requester_id,
        ticket_channel,
        agent_tier,
        distinct_agent_team_in_event_at_date_month,
        first_known_event_at_date,
        ticket_csat_share_of_event_at_date_month,
        prev_ticket_requester_id,
        first_known_ticket_requester_id,
        event_at_plus_15m_cume_dist,
        ticket_requester_id_vs_event_at_date_month_avg,
        ticket_csat_rank,
        ROUND(PERCENT_RANK() OVER w_event_at_date_month_by_disti, 3) AS distinct_agent_team_in_event_at_date_month_perce,
        NTH_VALUE(event_at_plus_15m_cume_dist, 2) OVER (PARTITION BY MONTH(event_at_date) ORDER BY distinct_agent_team_in_event_at_date_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_plus_15m_cume_dist,
        LEAD(event_at_date) OVER (PARTITION BY MONTH(event_at_date) ORDER BY distinct_agent_team_in_event_at_date_month, id) AS next_event_at_date,
        SUM(event_at_plus_15m_cume_dist) OVER (PARTITION BY ticket_id, ticket_requester_id_quartile) AS ticket_id_ticket_requester_id_quartile_sum_event
    FROM with_trends
    WINDOW w_event_at_date_month_by_disti AS (PARTITION BY MONTH(event_at_date) ORDER BY distinct_agent_team_in_event_at_date_month)
)
SELECT
    id,
    ticket_csat,
    ticket_requester_id,
    ticket_channel,
    ticket_csat_share_of_event_at_date_month,
    prev_ticket_requester_id,
    first_known_ticket_requester_id,
    event_at_plus_15m_cume_dist,
    ticket_requester_id_vs_event_at_date_month_avg,
    second_event_at_plus_15m_cume_dist,
    next_event_at_date,
    DATE_TRUNC('MONTH', event_at_plus_15m) AS event_at_plus_15m_month
FROM windowed
ORDER BY ticket_csat DESC NULLS FIRST
