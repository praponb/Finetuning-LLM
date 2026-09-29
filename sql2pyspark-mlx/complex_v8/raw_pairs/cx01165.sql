WITH base AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        CONCAT_WS(': ', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE channel IS NOT NULL
),
with_parent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.channel_priority_label,
        su.tier AS agent_tier
    FROM base s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        channel,
        agent_tier,
        LEAD(closed_at, 2) OVER w_closed_at_month_channel_by_c AS next2_closed_at,
        DENSE_RANK() OVER w_closed_at_year_by_closed_at AS closed_at_dense_rank,
        LAG(opened_at) OVER (PARTITION BY YEAR(closed_at) ORDER BY closed_at DESC, id) AS prev_opened_at,
        ROW_NUMBER() OVER (PARTITION BY agent_tier ORDER BY closed_at, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY YEAR(closed_at) ORDER BY closed_at DESC, id) AS closed_at_seq,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at), channel ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at,
        NTILE(5) OVER w_closed_at_month_channel_by_c AS closed_at_quintile
    FROM with_parent
    WINDOW w_closed_at_month_channel_by_c AS (PARTITION BY DATE_TRUNC('MONTH', closed_at), channel ORDER BY closed_at, id), w_closed_at_year_by_closed_at AS (PARTITION BY YEAR(closed_at) ORDER BY closed_at DESC, id)
)
-- Final projection
SELECT
    id,
    agent_id,
    channel,
    closed_at_dense_rank,
    closed_at_seq,
    first_known_opened_at,
    closed_at_quintile
FROM with_windows
ORDER BY closed_at_dense_rank, agent_id DESC
