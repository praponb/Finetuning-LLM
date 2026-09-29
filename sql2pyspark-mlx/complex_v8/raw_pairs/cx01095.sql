WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at
    FROM tickets
    WHERE csat BETWEEN 2 AND 4
),
with_parent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.days_since_closed_at,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM valid_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY agent_hired_on, id) AS seq_no,
        LAG(days_since_closed_at, 2) OVER (PARTITION BY channel ORDER BY agent_hired_on DESC, id) AS prev2_days_since_closed_at,
        COUNT(*) OVER w_agent_id_agent_hired_on_mont AS rows_in_agent_id_agent_hired_on_month,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_team ORDER BY opened_at NULLS LAST), 3) AS opened_at_cume_dist,
        NTILE(4) OVER (PARTITION BY channel ORDER BY agent_hired_on DESC, id) AS agent_hired_on_quartile,
        RANK() OVER (ORDER BY agent_hired_on) AS agent_hired_on_rank,
        COUNT(*) OVER w_agent_id AS rows_in_agent_id,
        DENSE_RANK() OVER w_all_by_agent_hired_on_ties AS agent_hired_on_dense_rank,
        MIN(days_since_closed_at) OVER (PARTITION BY agent_id, DATE_TRUNC('MONTH', agent_hired_on)) AS agent_id_agent_hired_on_month_min_days_since_clo,
        MAX(days_since_closed_at) OVER (PARTITION BY channel ORDER BY agent_hired_on DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS days_since_closed_at_max_6_prior,
        MIN(days_since_closed_at) OVER (PARTITION BY agent_id) AS agent_id_min_days_since_closed_at,
        FIRST_VALUE(agent_hired_on) OVER (ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_agent_hired_on
    FROM with_parent
    WINDOW w_agent_id_agent_hired_on_mont AS (PARTITION BY agent_id, DATE_TRUNC('MONTH', agent_hired_on)), w_agent_id AS (PARTITION BY agent_id), w_all_by_agent_hired_on_ties AS (ORDER BY agent_hired_on)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        opened_at,
        days_since_closed_at,
        agent_hired_on,
        rows_in_agent_id_agent_hired_on_month,
        opened_at_cume_dist,
        agent_hired_on_quartile,
        agent_hired_on_rank,
        agent_hired_on_dense_rank,
        days_since_closed_at_max_6_prior,
        agent_id_min_days_since_closed_at,
        first_agent_hired_on,
        CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref,
        FLOOR(agent_hired_on_quartile / 2) * 2 AS agent_hired_on_quartile_bucket,
        CONCAT_WS('-', agent_team, channel) AS agent_team_channel_label
    FROM metrics
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY agent_hired_on_dense_rank, agent_hired_on_rank) AS rows_in_agent_hired_on_dense_rank_agent_hired_on,
        SUM(agent_hired_on_quartile) OVER (ORDER BY agent_hired_on_quartile_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_agent_hired_on_quartile,
        COUNT(*) OVER (PARTITION BY agent_hired_on_dense_rank ORDER BY agent_hired_on_quartile_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY agent_hired_on_dense_rank)) AS distinct_agent_id_in_agent_hired_on_dense_rank,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (ORDER BY agent_hired_on_quartile_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        RANK() OVER (PARTITION BY MONTH(first_agent_hired_on), agent_hired_on_dense_rank ORDER BY agent_hired_on DESC, id) AS agent_hired_on_rank_2
    FROM tagged
    WINDOW w_agent_hired_on_dense_rank_ag AS (PARTITION BY agent_hired_on_dense_rank, agent_hired_on_rank)
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    days_since_closed_at,
    agent_hired_on,
    opened_at_cume_dist,
    agent_hired_on_quartile,
    agent_hired_on_rank,
    agent_hired_on_dense_rank,
    days_since_closed_at_max_6_prior,
    agent_id_min_days_since_closed_at,
    first_agent_hired_on,
    channel_ref,
    agent_hired_on_quartile_bucket,
    agent_team_channel_label,
    rows_in_agent_hired_on_dense_rank_agent_hired_on,
    running_agent_hired_on_quartile,
    running_count,
    last_known_opened_at,
    agent_hired_on_rank_2,
    COALESCE(days_since_closed_at, 0) AS days_since_closed_at_filled
FROM sequenced
ORDER BY agent_hired_on_rank NULLS LAST, last_known_opened_at DESC
