WITH recent AS (
    -- Filter support_agents
    SELECT
        id,
        team,
        tier,
        hired_on,
        CASE WHEN tier IN ('T3', 'T2') THEN 'group_a' WHEN tier = 'T1' THEN 'group_b' ELSE 'other' END AS tier_group
    FROM support_agents
    WHERE team = 'Tech'
      AND agent_name IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        team,
        tier,
        hired_on,
        tier_group,
        LAG(hired_on, 3) OVER (PARTITION BY tier, team ORDER BY hired_on DESC, id) AS prev3_hired_on,
        NTH_VALUE(hired_on, 2) OVER (ORDER BY hired_on NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on,
        ROUND(CUME_DIST() OVER w_all_by_hired_on_ties, 3) AS hired_on_cume_dist,
        LAST_VALUE(hired_on) OVER (ORDER BY hired_on NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_hired_on,
        ROUND(PERCENT_RANK() OVER (ORDER BY hired_on NULLS LAST), 3) AS hired_on_percent_rank
    FROM recent
    WINDOW w_all_by_hired_on_ties AS (ORDER BY hired_on NULLS LAST)
),
with_children AS (
    -- One row per ticket
    SELECT
        s.id,
        s.team,
        s.tier,
        s.tier_group,
        s.prev3_hired_on,
        s.second_hired_on,
        s.hired_on_percent_rank,
        t.id AS ticket_id,
        t.csat AS ticket_csat,
        t.priority AS ticket_priority,
        t.channel AS ticket_channel,
        t.closed_at AS ticket_closed_at
    FROM windowed s
    JOIN tickets t ON t.agent_id = s.id AND t.closed_at >= TIMESTAMP '2025-01-14 03:00:00'
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.tier,
        s.tier_group,
        s.prev3_hired_on,
        s.second_hired_on,
        s.hired_on_percent_rank,
        s.ticket_id,
        s.ticket_csat,
        s.ticket_priority,
        s.ticket_channel,
        s.ticket_closed_at,
        t.requester_id AS ticket_requester_id,
        t.agent_id AS ticket_agent_id
    FROM with_children s
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    COALESCE(ticket_priority, 'ALL') AS ticket_priority,
    ticket_agent_id,
    COALESCE(ticket_channel, 'ALL') AS ticket_channel,
    ROUND(SUM(ticket_requester_id) / NULLIF(SUM(hired_on_percent_rank), 0), 3) AS ticket_requester_id_to_hired_on_percent_rank_rat,
    ROUND(SUM(hired_on_percent_rank) / NULLIF(SUM(ticket_requester_id), 0), 3) AS hired_on_percent_rank_to_ticket_requester_id_rat
FROM enriched_ticket
GROUP BY GROUPING SETS ((ticket_priority, ticket_agent_id, ticket_channel), (ticket_priority), (ticket_agent_id))
