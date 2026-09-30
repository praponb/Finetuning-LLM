WITH scoped_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        LENGTH(TRIM(channel)) AS channel_len,
        DAYOFWEEK(opened_at) AS opened_at_dayofweek
    FROM tickets
    WHERE requester_id < 23
      AND agent_id <= 3
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        opened_at_dayofweek,
        NTILE(4) OVER w_all_by_csat AS csat_quartile,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY opened_at_dayofweek)) AS distinct_agent_id_in_opened_at_dayofweek,
        MIN(csat) OVER (PARTITION BY csat ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_csat_so_far,
        MAX(channel_len) OVER (PARTITION BY opened_at_dayofweek) AS opened_at_dayofweek_max_channel_len,
        LEAD(csat) OVER w_all_by_csat AS next_csat,
        LAST_VALUE(channel_len) OVER (PARTITION BY csat ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_channel_len,
        SUM(opened_at_dayofweek) OVER (PARTITION BY opened_at_dayofweek) AS opened_at_dayofweek_sum_opened_at_dayofweek,
        ROUND(PERCENT_RANK() OVER w_csat_by_closed_at_ties, 3) AS closed_at_percent_rank,
        DENSE_RANK() OVER (PARTITION BY channel, agent_id ORDER BY opened_at) AS opened_at_dense_rank,
        ROUND(csat / NULLIF(SUM(csat) OVER w_opened_at_dayofweek, 0), 4) AS csat_share_of_opened_at_dayofweek,
        ROUND(CUME_DIST() OVER (ORDER BY csat), 3) AS csat_cume_dist,
        LEAD(closed_at) OVER w_channel_agent_id_by_opened_a_2 AS next_closed_at,
        COUNT(*) OVER (PARTITION BY opened_at_dayofweek) AS rows_in_opened_at_dayofweek
    FROM scoped_tickets
    WINDOW w_all_by_csat AS (ORDER BY csat, id), w_csat_by_closed_at_ties AS (PARTITION BY csat ORDER BY closed_at DESC), w_opened_at_dayofweek AS (PARTITION BY opened_at_dayofweek), w_channel_agent_id_by_opened_a_2 AS (PARTITION BY channel, agent_id ORDER BY opened_at, id)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        opened_at_dayofweek,
        csat_quartile,
        distinct_agent_id_in_opened_at_dayofweek,
        min_csat_so_far,
        next_csat,
        last_channel_len,
        opened_at_dayofweek_sum_opened_at_dayofweek,
        opened_at_dense_rank,
        csat_cume_dist,
        ROUND(min_csat_so_far - AVG(min_csat_so_far) OVER w_channel, 2) AS min_csat_so_far_vs_channel_avg,
        ROUND(AVG(min_csat_so_far) OVER (PARTITION BY priority, opened_at_dense_rank ORDER BY opened_at_dayofweek_max_channel_len, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS min_csat_so_far_avg_1_prior,
        LAST_VALUE(csat_quartile) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat_quartile,
        DENSE_RANK() OVER (PARTITION BY YEAR(opened_at) ORDER BY closed_at_percent_rank, id) AS closed_at_percent_rank_dense_rank
    FROM ranked
    WINDOW w_channel AS (PARTITION BY channel)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        last_channel_len,
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_ym,
        opened_at_dayofweek,
        COUNT(*) AS row_count,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        MIN(opened_at) AS first_opened_at,
        MIN(opened_at_dayofweek) AS min_opened_at_dayofweek,
        MAX(closed_at) AS last_closed_at
    FROM with_windows
    GROUP BY last_channel_len, DATE_FORMAT(opened_at, 'yyyy-MM'), opened_at_dayofweek
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(distinct_agent_ids, 2) OVER (ORDER BY first_opened_at, last_channel_len, opened_at_ym, opened_at_dayofweek ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_agent_ids,
        LEAD(last_closed_at, 3) OVER w_all_by_first_opened_at AS next3_last_closed_at
    FROM rollup_base
    WINDOW w_all_by_first_opened_at AS (ORDER BY first_opened_at, last_channel_len, opened_at_ym, opened_at_dayofweek)
)
SELECT
    last_channel_len,
    opened_at_ym,
    opened_at_dayofweek,
    distinct_agent_ids,
    first_opened_at,
    min_opened_at_dayofweek,
    last_closed_at,
    second_distinct_agent_ids,
    CASE WHEN last_channel_len >= 10 THEN 'large' WHEN last_channel_len >= 5 THEN 'medium' ELSE 'small' END AS last_channel_len_tier
FROM windowed
ORDER BY distinct_agent_ids, last_channel_len, opened_at_ym, opened_at_dayofweek
LIMIT 10
