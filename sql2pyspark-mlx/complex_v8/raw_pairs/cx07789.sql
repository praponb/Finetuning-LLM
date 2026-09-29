WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 60, 1) AS minutes_closed_at_to_opened_at
    FROM tickets
    WHERE channel NOT IN ('email', 'phone')
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        su.agent_name,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM valid_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(csat) OVER w_csat AS csat_min_csat,
        NTILE(10) OVER (PARTITION BY priority, MONTH(closed_at) ORDER BY csat, id) AS csat_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY channel ORDER BY closed_at ASC, id), 3) AS closed_at_percent_rank,
        requester_id - LAG(requester_id) OVER (ORDER BY agent_hired_on DESC NULLS FIRST, id) AS requester_id_change,
        ROUND(AVG(csat) OVER w_csat, 2) AS csat_avg_csat,
        SUM(csat) OVER (PARTITION BY channel ORDER BY closed_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS csat_sum_3,
        SUM(csat) OVER (ORDER BY agent_hired_on DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        NTILE(5) OVER (ORDER BY agent_hired_on DESC NULLS FIRST, id) AS agent_hired_on_quintile,
        LEAD(agent_hired_on) OVER (ORDER BY requester_id, id) AS next_agent_hired_on,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY channel ORDER BY closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER w_agent_team, 0), 4) AS requester_id_share_of_agent_team,
        LEAD(closed_at, 3) OVER (ORDER BY requester_id, id) AS next3_closed_at
    FROM with_parent_agent
    WINDOW w_csat AS (PARTITION BY csat), w_agent_team AS (PARTITION BY agent_team)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        requester_id - closed_at_percent_rank AS requester_id_minus_closed_at_percent_rank,
        DATE_FORMAT(next3_closed_at, 'yyyy-MM') AS next3_closed_at_label,
        DATE_TRUNC('WEEK', agent_hired_on) AS agent_hired_on_week
    FROM windowed
),
summary AS (
    -- Summarise per group
    SELECT
        agent_hired_on_quintile,
        channel,
        YEAR(next_agent_hired_on) AS next_agent_hired_on_year,
        COUNT(*) FILTER (WHERE UPPER(TRIM(agent_name)) LIKE 'F%') AS n_upper_trim_agent_name_like_f,
        COUNT(*) AS row_count,
        ROUND(STDDEV(requester_id), 2) AS stddev_requester_id
    FROM derived
    GROUP BY agent_hired_on_quintile, channel, YEAR(next_agent_hired_on)
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    agent_hired_on_quintile,
    channel,
    next_agent_hired_on_year,
    n_upper_trim_agent_name_like_f,
    CONCAT(channel, '#', CAST(agent_hired_on_quintile AS STRING)) AS channel_ref
FROM summary
