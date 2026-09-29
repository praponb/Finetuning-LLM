WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT(channel, '#', CAST(agent_id AS STRING)) AS channel_ref
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-24 15:00:00'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        csat,
        tags,
        channel_ref,
        NTH_VALUE(csat, 2) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        MIN(csat) OVER w_channel AS channel_min_csat,
        MAX(csat) OVER w_channel AS channel_max_csat,
        ROW_NUMBER() OVER w_all_by_opened_at AS opened_at_seq,
        SUM(csat) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        LEAD(csat, 2) OVER (PARTITION BY priority, csat ORDER BY csat DESC, id) AS next2_csat
    FROM filtered
    WINDOW w_channel AS (PARTITION BY channel), w_all_by_opened_at AS (ORDER BY opened_at DESC, id)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        csat,
        tags,
        channel_ref,
        channel_min_csat,
        opened_at_seq,
        running_csat,
        NTILE(5) OVER (PARTITION BY channel ORDER BY next2_csat DESC, id) AS next2_csat_quintile,
        LAST_VALUE(channel_min_csat) IGNORE NULLS OVER (PARTITION BY opened_at_seq, next2_csat ORDER BY second_csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel_min_csat,
        RANK() OVER (PARTITION BY channel ORDER BY next2_csat DESC, id) AS next2_csat_rank,
        COUNT(*) OVER w_next2_csat_agent_id AS rows_in_next2_csat_agent_id,
        SUM(channel_max_csat) OVER (PARTITION BY next2_csat ORDER BY csat DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS channel_max_csat_sum_3_prior
    FROM with_windows
    WINDOW w_next2_csat_agent_id AS (PARTITION BY next2_csat, agent_id)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        TO_DATE(opened_at) AS opened_at_day,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        MAX(opened_at) AS last_opened_at,
        SUM(last_known_channel_min_csat) AS total_last_known_channel_min_csat,
        MIN(opened_at) AS first_opened_at
    FROM sequenced
    WHERE channel_min_csat < 28
    GROUP BY TO_DATE(opened_at)
    HAVING AVG(last_known_channel_min_csat) > 16
)
SELECT
    opened_at_day,
    last_opened_at,
    total_last_known_channel_min_csat,
    first_opened_at,
    TRUNC(opened_at_day, 'MM') AS opened_at_day_month_start
FROM rollup_base
WHERE total_last_known_channel_min_csat <= 180
