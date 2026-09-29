WITH recent AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug
    FROM support_agents
    WHERE agent_name IS NOT NULL
      AND YEAR(hired_on) = 2024
),
latest AS (
    -- Keep the latest row per team
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        team_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY hired_on DESC, id DESC) AS rn
        FROM recent
    ) ranked
    WHERE rn = 1
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        team_slug,
        LENGTH(TRIM(agent_name)) AS agent_name_len,
        LAST_DAY(hired_on) AS hired_on_month_end,
        YEAR(hired_on) AS hired_on_year
    FROM latest
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) AS ticket_count,
        COUNT(*) AS ticket_count_2,
        COUNT(*) AS ticket_count_3
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        COALESCE(t.ticket_count_2, 0) AS ticket_count_2,
        COALESCE(t.ticket_count_3, 0) AS ticket_count_3
    FROM scored s
    LEFT JOIN ticket_stats t ON t.agent_id = s.id
),
with_children_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.team,
        s.hired_on,
        s.agent_name_len,
        s.ticket_count,
        s.ticket_count_2,
        s.ticket_count_3,
        t.id AS ticket_id,
        t.opened_at AS ticket_opened_at,
        t.channel AS ticket_channel,
        t.requester_id AS ticket_requester_id
    FROM profiled s
    LEFT JOIN tickets t ON t.agent_id = s.id AND t.requester_id > 3
)
SELECT
    ticket_count_2,
    agent_name_len,
    COALESCE(ticket_channel, 'ALL') AS ticket_channel,
    ARRAY_SORT(COLLECT_SET(ticket_channel)) AS ticket_channel_list,
    MIN(hired_on) AS first_hired_on
FROM with_children_tickets
GROUP BY CUBE(ticket_count_2, agent_name_len, ticket_channel)
ORDER BY ticket_count_2 NULLS LAST, agent_name_len NULLS LAST, ticket_channel NULLS LAST
