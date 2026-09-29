WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE priority <> 'P3'
),
combined_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.csat,
        su.team AS agent_team
    FROM valid s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team IN ('VIP', 'Onboarding', 'Tech')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_at,
        agent_team,
        MIN(csat) OVER w_agent_id_agent_team AS agent_id_agent_team_min_csat,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority ORDER BY closed_at, id), 3) AS closed_at_percent_rank,
        ROUND(csat - AVG(csat) OVER (PARTITION BY agent_id, agent_team), 2) AS csat_vs_agent_id_agent_team_avg,
        NTILE(3) OVER (PARTITION BY agent_team ORDER BY closed_at NULLS LAST, id) AS closed_at_tercile,
        FIRST_VALUE(csat) OVER (PARTITION BY priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), agent_id)) AS distinct_agent_team_in_opened_at_month_agent_id,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        LEAD(csat) OVER w_agent_team_by_closed_at AS next_csat,
        COUNT(*) OVER (PARTITION BY agent_id, agent_team) AS rows_in_agent_id_agent_team,
        ROW_NUMBER() OVER (ORDER BY closed_at, id) AS seq_no,
        RANK() OVER (PARTITION BY agent_team ORDER BY closed_at NULLS LAST) AS closed_at_rank,
        LEAD(csat, 3) OVER w_all_by_closed_at_2 AS next3_csat,
        COUNT(*) OVER w_opened_at_month_agent_id AS rows_in_opened_at_month_agent_id
    FROM combined_details_agent
    WINDOW w_agent_id_agent_team AS (PARTITION BY agent_id, agent_team), w_agent_team_by_closed_at AS (PARTITION BY agent_team ORDER BY closed_at NULLS LAST, id), w_all_by_closed_at_2 AS (ORDER BY closed_at DESC, id), w_opened_at_month_agent_id AS (PARTITION BY DATE_TRUNC('MONTH', opened_at), agent_id)
)
-- Final projection
SELECT
    id,
    opened_at,
    agent_id_agent_team_min_csat,
    closed_at_percent_rank,
    first_csat,
    second_closed_at,
    next_csat,
    rows_in_agent_id_agent_team,
    seq_no,
    rows_in_opened_at_month_agent_id,
    ABS(closed_at_percent_rank - 0.7) AS closed_at_percent_rank_distance,
    COALESCE(agent_team, 'UNKNOWN') AS agent_team_or_unknown
FROM sequenced
ORDER BY id DESC, closed_at_percent_rank DESC
LIMIT 5
