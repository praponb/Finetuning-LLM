WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd') AS closed_at_label
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-13 12:00:00'
),
joined_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.csat,
        su.team AS agent_team
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY agent_team)) AS distinct_agent_id_in_agent_team,
        ROUND(csat / NULLIF(SUM(csat) OVER w_channel, 0), 4) AS csat_share_of_channel,
        SUM(csat) OVER (PARTITION BY csat) AS csat_sum_csat,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        MAX(csat) OVER w_channel AS channel_max_csat,
        LAST_VALUE(csat) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY agent_id, agent_team ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at,
        RANK() OVER w_csat_agent_team_by_opened_at AS opened_at_rank,
        LAST_VALUE(opened_at) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER w_opened_at_month_by_opened_at)) / 60, 1) AS minutes_since_prev_opened_at,
        DENSE_RANK() OVER w_agent_id_by_opened_at_ties AS opened_at_dense_rank,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at) ORDER BY opened_at ASC NULLS LAST, id) AS opened_at_rank_2,
        FIRST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat,
        FIRST_VALUE(csat) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        FIRST_VALUE(csat) OVER (ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat_2,
        FIRST_VALUE(opened_at) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at) ORDER BY opened_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_at
    FROM joined_agent
    WINDOW w_channel AS (PARTITION BY channel), w_csat_agent_team_by_opened_at AS (PARTITION BY csat, agent_team ORDER BY opened_at NULLS LAST), w_opened_at_month_by_opened_at AS (PARTITION BY DATE_TRUNC('MONTH', opened_at) ORDER BY opened_at ASC NULLS LAST, id), w_agent_id_by_opened_at_ties AS (PARTITION BY agent_id ORDER BY opened_at)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN channel IN ('chat', 'phone') THEN 'group_a' WHEN channel = 'web' THEN 'group_b' ELSE 'other' END AS channel_group,
        opened_at + INTERVAL 15 MINUTES AS opened_at_plus_15m
    FROM sequenced
    WHERE opened_at_rank < 7
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        channel,
        csat_sum_csat,
        channel_max_csat,
        first_known_opened_at,
        opened_at_rank,
        last_opened_at,
        minutes_since_prev_opened_at,
        opened_at_rank_2,
        first_known_csat,
        first_csat,
        first_opened_at,
        channel_group,
        ROUND(PERCENT_RANK() OVER (ORDER BY opened_at NULLS LAST), 3) AS opened_at_percent_rank,
        FIRST_VALUE(opened_at_plus_15m) IGNORE NULLS OVER (PARTITION BY distinct_agent_id_in_agent_team, DATE_TRUNC('MONTH', last_known_opened_at) ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at_plus_15m,
        DENSE_RANK() OVER (ORDER BY opened_at NULLS LAST) AS opened_at_dense_rank_2
    FROM annotated
),
rollup_base AS (
    -- Group-level totals
    SELECT
        opened_at_rank_2,
        DATE_TRUNC('MONTH', first_known_opened_at_plus_15m) AS first_known_opened_at_plus_15m_month,
        channel_group,
        MIN(opened_at_dense_rank_2) AS min_opened_at_dense_rank_2,
        COUNT(*) AS n_rows
    FROM sequenced_2
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(min_opened_at_dense_rank_2 / NULLIF(SUM(min_opened_at_dense_rank_2) OVER (PARTITION BY opened_at_rank_2, min_opened_at_dense_rank_2), 0), 4) AS min_opened_at_dense_rank_2_share_of_opened_at_ra
    FROM rollup_base
)
-- Final projection
SELECT
    opened_at_rank_2,
    first_known_opened_at_plus_15m_month,
    min_opened_at_dense_rank_2,
    INITCAP(TRIM(channel_group)) AS channel_group_clean
FROM windowed
ORDER BY opened_at_rank_2 DESC NULLS FIRST
