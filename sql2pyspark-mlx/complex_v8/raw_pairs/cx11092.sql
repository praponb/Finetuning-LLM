WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        tags,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        CASE WHEN channel IN ('web', 'email') THEN 'group_a' WHEN channel = 'chat' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE (closed_at >= TIMESTAMP '2025-02-08 13:00:00' OR agent_id >= 5)
),
combined_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.tags,
        s.channel_group,
        su.hired_on AS agent_hired_on
    FROM recent_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on) ORDER BY closed_at, id) AS closed_at_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(agent_hired_on), channel_group ORDER BY agent_hired_on DESC), 3) AS agent_hired_on_percent_rank,
        LAG(closed_at) OVER w_agent_hired_on_year_channel_2 AS prev_closed_at,
        DENSE_RANK() OVER w_agent_hired_on_year_channel AS agent_hired_on_dense_rank,
        LAST_VALUE(agent_hired_on) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_agent_hired_on,
        ROW_NUMBER() OVER w_agent_hired_on_month_by_clos AS seq_no,
        DENSE_RANK() OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS closed_at_dense_rank,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER w_agent_hired_on_month_by_clos) AS days_since_prev_agent_hired_on,
        LEAD(closed_at, 2) OVER (PARTITION BY agent_id, priority ORDER BY closed_at ASC, id) AS next2_closed_at,
        LAG(closed_at, 3) OVER (PARTITION BY channel_group ORDER BY closed_at DESC, id) AS prev3_closed_at,
        DENSE_RANK() OVER w_agent_id_priority_by_closed AS closed_at_dense_rank_2
    FROM combined_details_agent
    WINDOW w_agent_hired_on_year_channel_2 AS (PARTITION BY YEAR(agent_hired_on), channel_group ORDER BY agent_hired_on DESC, id), w_agent_hired_on_year_channel AS (PARTITION BY YEAR(agent_hired_on), channel_group ORDER BY agent_hired_on DESC), w_agent_hired_on_month_by_clos AS (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on) ORDER BY closed_at, id), w_agent_id_priority_by_closed AS (PARTITION BY agent_id, priority ORDER BY closed_at ASC, id)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        closed_at_dense_rank,
        YEAR(next2_closed_at) AS next2_closed_at_year,
        DATE_FORMAT(agent_hired_on, 'yyyy-MM') AS agent_hired_on_ym,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list
    FROM with_trends
    GROUP BY closed_at_dense_rank, YEAR(next2_closed_at), DATE_FORMAT(agent_hired_on, 'yyyy-MM')
    HAVING MAX(closed_at_decile) > 2
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(next2_closed_at_year) OVER (PARTITION BY next2_closed_at_year) AS next2_closed_at_year_max_next2_closed_at_year,
        MIN(next2_closed_at_year) OVER (PARTITION BY next2_closed_at_year) AS next2_closed_at_year_min_next2_closed_at_year,
        MAX(closed_at_dense_rank) OVER w_next2_closed_at_year AS next2_closed_at_year_max_closed_at_dense_rank,
        ROUND(AVG(next2_closed_at_year) OVER (PARTITION BY next2_closed_at_year), 2) AS next2_closed_at_year_avg_next2_closed_at_year
    FROM rollup_base
    WINDOW w_next2_closed_at_year AS (PARTITION BY next2_closed_at_year)
)
SELECT
    closed_at_dense_rank,
    next2_closed_at_year,
    agent_hired_on_ym,
    priority_list,
    next2_closed_at_year_min_next2_closed_at_year,
    next2_closed_at_year_max_closed_at_dense_rank,
    next2_closed_at_year_avg_next2_closed_at_year,
    FLOOR(closed_at_dense_rank / 2) * 2 AS closed_at_dense_rank_bucket,
    CASE WHEN next2_closed_at_year >= 2025 THEN 'HIGH' WHEN next2_closed_at_year >= 2024 THEN 'MEDIUM' ELSE 'LOW' END AS next2_closed_at_year_tier
FROM with_trends_2
ORDER BY next2_closed_at_year_min_next2_closed_at_year DESC, closed_at_dense_rank, next2_closed_at_year, agent_hired_on_ym
LIMIT 5
