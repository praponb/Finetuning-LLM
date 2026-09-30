WITH valid AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        QUARTER(hired_on) AS hired_on_quarter,
        CONCAT_WS(': ', tier, agent_name) AS tier_agent_name_label,
        UPPER(TRIM(team)) AS team_clean
    FROM support_agents
    WHERE team IN ('VIP', 'Tech')
      AND UPPER(TRIM(agent_name)) LIKE 'H%'
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        QUARTER(hired_on) AS hired_on_quarter_2,
        LOWER(REGEXP_REPLACE(TRIM(team_clean), ' ', '_')) AS team_clean_slug
    FROM valid
    WHERE tier_agent_name_label IS NOT NULL
),
exploded_detail AS (
    -- One row per ticket
    SELECT
        s.id,
        s.agent_name,
        s.team,
        s.tier_agent_name_label,
        s.hired_on_quarter_2,
        s.team_clean_slug,
        t.id AS ticket_id,
        t.opened_at AS ticket_opened_at,
        t.channel AS ticket_channel,
        t.csat AS ticket_csat
    FROM tagged s
    JOIN tickets t ON t.agent_id = s.id AND t.opened_at >= TIMESTAMP '2025-01-25 11:00:00'
),
line_level AS (
    -- One row per ticket
    SELECT
        s.id,
        s.agent_name,
        s.team,
        s.hired_on_quarter_2,
        s.team_clean_slug,
        s.ticket_id,
        s.ticket_opened_at,
        s.ticket_csat,
        t.id AS ticket_id_2,
        t.channel AS ticket_channel_2,
        t.tags AS ticket_tags,
        t.requester_id AS ticket_requester_id
    FROM exploded_detail s
    JOIN tickets t ON t.agent_id = s.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(hired_on_quarter_2 / NULLIF(SUM(hired_on_quarter_2) OVER (PARTITION BY ticket_channel_2), 0), 4) AS hired_on_quarter_2_share_of_ticket_channel_2,
        SUM(hired_on_quarter_2) OVER (PARTITION BY ticket_id_2 ORDER BY ticket_requester_id DESC, id, ticket_id, ticket_id_2 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS hired_on_quarter_2_sum_3
    FROM line_level
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.team,
        s.hired_on_quarter_2,
        s.team_clean_slug,
        s.ticket_id,
        s.ticket_opened_at,
        s.ticket_id_2,
        s.ticket_channel_2,
        s.ticket_requester_id,
        s.hired_on_quarter_2_share_of_ticket_channel_2,
        s.hired_on_quarter_2_sum_3,
        t.channel AS ticket_channel,
        t.priority AS ticket_priority,
        su.agent_name AS agent_agent_name,
        su.hired_on AS agent_hired_on
    FROM with_windows s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
with_details_ticket_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.team,
        s.hired_on_quarter_2,
        s.ticket_id,
        s.ticket_opened_at,
        s.ticket_id_2,
        s.ticket_channel_2,
        s.ticket_requester_id,
        s.hired_on_quarter_2_share_of_ticket_channel_2,
        s.hired_on_quarter_2_sum_3,
        s.ticket_channel,
        s.ticket_priority,
        s.agent_agent_name,
        s.agent_hired_on,
        t.channel AS ticket_channel_3,
        t.agent_id AS ticket_agent_id
    FROM with_details_ticket s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    team,
    hired_on_quarter_2,
    ticket_opened_at,
    ticket_id_2,
    ticket_channel_2,
    ticket_requester_id,
    hired_on_quarter_2_share_of_ticket_channel_2,
    ticket_channel,
    ticket_priority,
    agent_agent_name,
    agent_hired_on,
    ticket_channel_3,
    COALESCE(ticket_channel_2, 'UNKNOWN') AS ticket_channel_2_or_unknown
FROM with_details_ticket_2
WHERE hired_on_quarter_2_sum_3 <= 9
