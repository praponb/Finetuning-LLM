WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id_2
    FROM tickets
    WHERE requester_id >= 6
      AND priority = 'P2'
),
combined_details_agent AS (
    -- Join support_agents
    SELECT s.*, su.tier AS agent_tier
    FROM base_tickets s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier <> 'T2'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        csat_per_requester_id,
        csat_per_requester_id_2,
        agent_tier,
        LEAD(closed_at) OVER (PARTITION BY channel, agent_id ORDER BY closed_at, id) AS next_closed_at,
        ROUND(CUME_DIST() OVER w_all_by_closed_at_ties, 3) AS closed_at_cume_dist,
        MAX(csat_per_requester_id_2) OVER (PARTITION BY YEAR(closed_at), channel) AS closed_at_year_channel_max_csat_per_requester_id,
        MIN(csat_per_requester_id) OVER (PARTITION BY YEAR(closed_at), channel) AS closed_at_year_channel_min_csat_per_requester_id,
        MIN(csat_per_requester_id_2) OVER (PARTITION BY channel, agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_csat_per_requester_id_2_so_far,
        SIZE(COLLECT_SET(channel) OVER w_agent_id_agent_tier) AS distinct_channel_in_agent_id_agent_tier,
        FIRST_VALUE(csat_per_requester_id) OVER (PARTITION BY channel, agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat_per_requester_id,
        RANK() OVER (PARTITION BY channel ORDER BY opened_at DESC) AS opened_at_rank,
        ROUND(csat_per_requester_id_2 - AVG(csat_per_requester_id_2) OVER (PARTITION BY YEAR(closed_at), channel), 2) AS csat_per_requester_id_2_vs_closed_at_year_channe,
        COUNT(*) OVER w_closed_at_year AS rows_in_closed_at_year,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY opened_at DESC) AS opened_at_dense_rank
    FROM combined_details_agent
    WINDOW w_all_by_closed_at_ties AS (ORDER BY closed_at), w_agent_id_agent_tier AS (PARTITION BY agent_id, agent_tier), w_closed_at_year AS (PARTITION BY YEAR(closed_at))
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        UPPER(TRIM(channel)) AS channel_clean,
        UPPER(TRIM(channel)) AS channel_clean_2
    FROM sequenced
    WHERE csat_per_requester_id > 1.5
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY closed_at ASC) AS closed_at_dense_rank,
        NTILE(3) OVER (ORDER BY closed_at ASC, id) AS closed_at_tercile,
        FIRST_VALUE(next_closed_at) IGNORE NULLS OVER (ORDER BY closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_closed_at,
        ROUND(csat_per_requester_id_2 / NULLIF(SUM(csat_per_requester_id_2) OVER (PARTITION BY agent_tier, opened_at_rank), 0), 4) AS csat_per_requester_id_2_share_of_agent_tier_open,
        SIZE(COLLECT_SET(channel) OVER w_agent_tier_opened_at_rank) AS distinct_channel_in_agent_tier_opened_at_rank,
        ROUND(AVG(csat_per_requester_id_2) OVER (PARTITION BY agent_tier, opened_at_rank), 2) AS agent_tier_opened_at_rank_avg_csat_per_requester,
        FIRST_VALUE(next_closed_at) OVER (PARTITION BY agent_tier ORDER BY csat_per_requester_id_2 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_next_closed_at,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_tier ORDER BY csat_per_requester_id_2 ASC, id), 3) AS csat_per_requester_id_2_cume_dist
    FROM labeled
    WINDOW w_agent_tier_opened_at_rank AS (PARTITION BY agent_tier, opened_at_rank)
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', next_closed_at) AS next_closed_at_month,
        channel,
        COUNT(DISTINCT channel) AS distinct_channels,
        MAX(closed_at) AS last_closed_at
    FROM with_windows
    GROUP BY DATE_TRUNC('MONTH', next_closed_at), channel
    HAVING SUM(first_csat_per_requester_id) > 3.5
)
-- Final projection
SELECT
    next_closed_at_month,
    distinct_channels,
    last_closed_at,
    ROUND((UNIX_TIMESTAMP(last_closed_at) - UNIX_TIMESTAMP(next_closed_at_month)) / 3600, 1) AS hours_next_closed_at_month_to_last_closed_at,
    DAYOFWEEK(last_closed_at) AS last_closed_at_dayofweek
FROM totals
WHERE TO_DATE(last_closed_at) < DATE '2025-02-26'
ORDER BY last_closed_at
