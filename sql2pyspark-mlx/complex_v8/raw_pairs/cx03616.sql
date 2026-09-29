WITH scoped_agents AS (
    -- Scope the support_agents rows
    SELECT
        id,
        team,
        tier,
        hired_on,
        SUBSTRING(TRIM(agent_name), 1, 2) AS agent_name_prefix,
        CASE WHEN tier IN ('T1', 'T3') THEN 'group_a' WHEN tier = 'T2' THEN 'group_b' ELSE 'other' END AS tier_group
    FROM support_agents
    WHERE team = 'Tech'
),
detail_rows AS (
    -- One row per ticket
    SELECT
        s.id,
        s.hired_on,
        s.tier_group,
        t.id AS ticket_id,
        t.tags AS ticket_tags,
        t.channel AS ticket_channel,
        t.closed_at AS ticket_closed_at
    FROM scoped_agents s
    JOIN tickets t ON t.agent_id = s.id
),
without_matches AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM detail_rows s
    WHERE s.id IN (
            SELECT t.agent_id
            FROM tickets t
            WHERE SIZE(t.tags) >= 1
        )
      AND s.hired_on >= DATE '2024-07-09'
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', ticket_channel, tier_group) AS ticket_channel_tier_group_label
    FROM without_matches
)
SELECT
    id,
    hired_on,
    tier_group,
    ticket_id,
    ticket_tags,
    ticket_channel,
    ticket_closed_at,
    ticket_channel_tier_group_label,
    TO_DATE(ticket_closed_at) AS ticket_closed_at_date,
    DATE_TRUNC('MONTH', ticket_closed_at) AS ticket_closed_at_month
FROM enriched
