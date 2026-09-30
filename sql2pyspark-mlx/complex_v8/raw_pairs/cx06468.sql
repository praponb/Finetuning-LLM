WITH base AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        DATE_TRUNC('YEAR', closed_at) AS closed_at_year
    FROM tickets
    WHERE csat >= 2
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.closed_at_year,
        su.agent_name,
        su.tier AS agent_tier
    FROM base s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(closed_at_year) OVER (PARTITION BY channel ORDER BY closed_at_year DESC, id) AS next_closed_at_year,
        ROW_NUMBER() OVER w_all_by_opened_at AS opened_at_seq,
        NTILE(10) OVER (ORDER BY opened_at DESC, id) AS opened_at_decile,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id, channel ORDER BY opened_at, id), 3) AS opened_at_cume_dist,
        DENSE_RANK() OVER w_closed_at_year_month_channel AS opened_at_dense_rank,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY channel ORDER BY closed_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        LAG(closed_at_year) OVER (PARTITION BY agent_id, channel ORDER BY opened_at, id) AS prev_closed_at_year,
        ROUND(PERCENT_RANK() OVER w_agent_id_channel_by_opened_a, 3) AS opened_at_percent_rank,
        ROUND((UNIX_TIMESTAMP(closed_at_year) - UNIX_TIMESTAMP(LAG(closed_at_year) OVER w_agent_id_channel_by_opened_a)) / 60, 1) AS minutes_since_prev_closed_at_year
    FROM enriched_agent
    WINDOW w_all_by_opened_at AS (ORDER BY opened_at DESC, id), w_closed_at_year_month_channel AS (PARTITION BY MONTH(closed_at_year), channel ORDER BY opened_at DESC, id), w_agent_id_channel_by_opened_a AS (PARTITION BY agent_id, channel ORDER BY opened_at, id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY channel ORDER BY prev_closed_at_year, id) AS prev_closed_at_year_rank,
        ROUND(AVG(opened_at_seq) OVER (PARTITION BY opened_at_seq, TO_DATE(last_known_opened_at) ORDER BY opened_at_seq ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS opened_at_seq_avg_1_prior,
        NTILE(10) OVER (PARTITION BY channel ORDER BY prev_closed_at_year, id) AS prev_closed_at_year_decile,
        DENSE_RANK() OVER (PARTITION BY opened_at_seq, TO_DATE(last_known_opened_at) ORDER BY opened_at_seq ASC) AS opened_at_seq_dense_rank,
        MAX(opened_at_seq) OVER (PARTITION BY opened_at_seq, opened_at_decile) AS opened_at_seq_opened_at_decile_max_opened_at_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY channel ORDER BY prev_closed_at_year), 3) AS prev_closed_at_year_percent_rank,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY prev_closed_at_year, id) AS prev_closed_at_year_dense_rank,
        COUNT(*) OVER w_opened_at_seq_opened_at_deci AS rows_in_opened_at_seq_opened_at_decile
    FROM with_trends
    WINDOW w_opened_at_seq_opened_at_deci AS (PARTITION BY opened_at_seq, opened_at_decile)
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_ym,
        agent_id,
        agent_tier,
        MIN(closed_at_year) AS first_closed_at_year,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
        ROUND(SUM(opened_at_cume_dist), 2) AS total_opened_at_cume_dist,
        ROUND(STDDEV(opened_at_percent_rank), 2) AS stddev_opened_at_percent_rank,
        COUNT(DISTINCT agent_tier) AS distinct_agent_tiers,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list
    FROM windowed
    GROUP BY DATE_FORMAT(opened_at, 'yyyy-MM'), agent_id, agent_tier
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        opened_at_ym,
        agent_id,
        agent_tier,
        agent_name_list,
        total_opened_at_cume_dist,
        stddev_opened_at_percent_rank,
        distinct_agent_tiers,
        channel_list,
        ROUND(CUME_DIST() OVER w_all_by_total_opened_at_cume, 3) AS total_opened_at_cume_dist_cume_dist,
        LAST_VALUE(distinct_agent_tiers) IGNORE NULLS OVER (ORDER BY total_opened_at_cume_dist ASC, opened_at_ym, agent_id, agent_tier ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_distinct_agent_tiers
    FROM summary
    WINDOW w_all_by_total_opened_at_cume AS (ORDER BY total_opened_at_cume_dist ASC, opened_at_ym, agent_id, agent_tier)
)
-- Final projection
SELECT
    opened_at_ym,
    agent_id,
    agent_tier,
    total_opened_at_cume_dist,
    distinct_agent_tiers,
    channel_list,
    total_opened_at_cume_dist_cume_dist,
    last_known_distinct_agent_tiers,
    LENGTH(TRIM(agent_tier)) AS agent_tier_len
FROM with_trends_2
WHERE SIZE(agent_name_list) >= 1
ORDER BY distinct_agent_tiers DESC, agent_tier
