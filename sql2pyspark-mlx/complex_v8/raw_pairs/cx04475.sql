WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
),
rollup_base AS (
    -- Summarise per group
    SELECT
        agent_id,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        csat,
        COUNT(*) AS cnt,
        MAX(csat) AS max_csat,
        MIN(closed_at) AS first_closed_at,
        COUNT(DISTINCT channel) AS distinct_channels,
        COUNT(DISTINCT id) AS distinct_ids
    FROM recent_tickets
    WHERE opened_at < TIMESTAMP '2025-02-12 10:00:00'
    GROUP BY 1, 2, 3
),
with_parent AS (
    -- Attach agent attributes
    SELECT s.*, su.agent_name, su.tier AS agent_tier
    FROM rollup_base s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier <> 'T1'
),
tagged AS (
    -- Add calculated columns
    SELECT
        agent_id,
        opened_at_month,
        csat,
        max_csat,
        first_closed_at,
        distinct_channels,
        distinct_ids,
        agent_name,
        agent_tier,
        INITCAP(TRIM(agent_name)) AS agent_name_clean,
        CASE WHEN agent_tier IN ('T2', 'T1') THEN 'group_a' WHEN agent_tier = 'T3' THEN 'group_b' ELSE 'other' END AS agent_tier_group
    FROM with_parent
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.agent_id,
        s.opened_at_month,
        s.csat,
        s.first_closed_at,
        s.distinct_channels,
        s.agent_name,
        s.agent_tier,
        s.agent_name_clean,
        s.agent_tier_group,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier_2,
        su.team AS agent_team
    FROM tagged s
    JOIN support_agents su ON su.id = s.agent_id
),
line_level AS (
    -- One row per ticket
    SELECT
        s.agent_id,
        s.opened_at_month,
        s.csat,
        s.first_closed_at,
        s.agent_name,
        s.agent_tier_group,
        s.agent_hired_on,
        t.id AS ticket_id,
        t.closed_at AS ticket_closed_at,
        t.opened_at AS ticket_opened_at
    FROM enriched_agent s
    LEFT JOIN tickets t ON t.agent_id = s.agent_id AND TO_DATE(t.closed_at) < DATE '2025-03-08'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_tier_group, DATE_TRUNC('MONTH', first_closed_at) ORDER BY first_closed_at NULLS LAST), 3) AS first_closed_at_percent_rank,
        SUM(csat) OVER (PARTITION BY MONTH(agent_hired_on), DATE_TRUNC('MONTH', ticket_opened_at)) AS agent_hired_on_month_ticket_opened_at_month_sum_,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY MONTH(agent_hired_on), DATE_TRUNC('MONTH', ticket_opened_at)), 0), 4) AS csat_share_of_agent_hired_on_month_ticket_opened,
        ROUND(csat - AVG(csat) OVER (PARTITION BY MONTH(agent_hired_on), DATE_TRUNC('MONTH', ticket_opened_at)), 2) AS csat_vs_agent_hired_on_month_ticket_opened_at_mo,
        MAX(csat) OVER (PARTITION BY agent_tier_group, DATE_TRUNC('MONTH', first_closed_at) ORDER BY first_closed_at NULLS LAST, agent_id, opened_at_month, csat, ticket_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS csat_max_6_prior
    FROM line_level
)
-- Final projection
SELECT
    agent_id,
    opened_at_month,
    csat,
    first_closed_at,
    agent_name,
    ticket_id,
    ticket_closed_at,
    ticket_opened_at,
    first_closed_at_percent_rank,
    csat_share_of_agent_hired_on_month_ticket_opened,
    csat_vs_agent_hired_on_month_ticket_opened_at_mo,
    csat_max_6_prior,
    LOWER(TRIM(agent_tier_group)) AS agent_tier_group_clean
FROM sequenced
WHERE opened_at_month >= TIMESTAMP '2025-01-09 09:00:00'
ORDER BY ticket_opened_at DESC
