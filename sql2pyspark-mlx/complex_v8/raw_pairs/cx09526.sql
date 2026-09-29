WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        tags,
        LENGTH(TRIM(priority)) AS priority_len,
        ABS(requester_id - 12) AS requester_id_distance,
        DATE_TRUNC('MONTH', closed_at) AS closed_at_month
    FROM tickets
    WHERE NOT ARRAY_CONTAINS(tags, 'login')
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        YEAR(closed_at) AS closed_at_year,
        SUBSTRING(TRIM(channel), 1, 4) AS channel_prefix
    FROM filtered
),
with_parent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.tags,
        s.priority_len,
        s.requester_id_distance,
        s.closed_at_month,
        s.channel_or_unknown,
        s.channel_prefix,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM labeled s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        closed_at,
        tags,
        priority_len,
        closed_at_month,
        channel_prefix,
        agent_hired_on,
        ROUND(priority_len - AVG(priority_len) OVER (PARTITION BY agent_tier), 2) AS priority_len_vs_agent_tier_avg,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id, priority_len ORDER BY closed_at_month, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_6_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id, priority_len ORDER BY closed_at_month, id), 3) AS closed_at_month_percent_rank
    FROM with_parent
),
with_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.tags,
        s.priority_len,
        s.channel_prefix,
        s.priority_len_vs_agent_tier_avg,
        s.requester_id_avg_6_prior,
        s.closed_at_month_percent_rank,
        su.hired_on AS agent_hired_on_2,
        su.agent_name,
        su.team AS agent_team
    FROM with_windows s
    JOIN support_agents su ON su.id = s.agent_id
),
labeled_2 AS (
    -- Label and score rows
    SELECT *, ADD_MONTHS(agent_hired_on_2, 1) AS agent_hired_on_2_plus_1m
    FROM with_details_agent
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM labeled_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor = 'agent'
        )
      AND UPPER(TRIM(s.agent_name)) LIKE 'L%'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT agent_id, agent_team, closed_at_month_percent_rank
    FROM without_matches
) src
PIVOT (
    AVG(closed_at_month_percent_rank)
    FOR agent_team IN ('Onboarding', 'VIP')
)
ORDER BY agent_id
