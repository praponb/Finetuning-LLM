WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        csat,
        tags,
        DAYOFWEEK(opened_at) AS opened_at_dayofweek,
        GREATEST(csat, requester_id) AS greatest_csat_requester_id
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-03-04 11:00:00'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(csat) OVER (PARTITION BY opened_at_dayofweek, agent_id) AS opened_at_dayofweek_agent_id_min_csat,
        ROUND(AVG(csat) OVER (ORDER BY opened_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS csat_avg_3,
        ROUND(AVG(opened_at_dayofweek) OVER (PARTITION BY channel, opened_at_dayofweek ORDER BY csat, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS opened_at_dayofweek_avg_3_prior,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY opened_at_dayofweek, agent_id)) AS distinct_channel_in_opened_at_dayofweek_agent_id,
        ROUND(greatest_csat_requester_id / NULLIF(SUM(greatest_csat_requester_id) OVER (PARTITION BY opened_at_dayofweek, agent_id), 0), 4) AS greatest_csat_requester_id_share_of_opened_at_da,
        greatest_csat_requester_id - LAG(greatest_csat_requester_id) OVER w_channel_opened_at_dayofweek_2 AS greatest_csat_requester_id_change,
        ROUND(opened_at_dayofweek / NULLIF(SUM(opened_at_dayofweek) OVER w_opened_at_dayofweek, 0), 4) AS opened_at_dayofweek_share_of_opened_at_dayofweek,
        DENSE_RANK() OVER (PARTITION BY channel, opened_at_dayofweek ORDER BY csat) AS csat_dense_rank,
        LAG(opened_at, 2) OVER w_all_by_opened_at AS prev2_opened_at,
        NTILE(5) OVER (PARTITION BY csat, agent_id ORDER BY greatest_csat_requester_id DESC, id) AS greatest_csat_requester_id_quintile,
        NTILE(3) OVER (ORDER BY opened_at, id) AS opened_at_tercile,
        LAST_VALUE(csat) OVER (PARTITION BY csat, agent_id ORDER BY greatest_csat_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat,
        NTILE(3) OVER (PARTITION BY csat, agent_id ORDER BY greatest_csat_requester_id DESC, id) AS greatest_csat_requester_id_tercile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY channel, opened_at_dayofweek ORDER BY csat, id), 3) AS csat_percent_rank
    FROM valid
    WINDOW w_channel_opened_at_dayofweek_2 AS (PARTITION BY channel, opened_at_dayofweek ORDER BY csat, id), w_opened_at_dayofweek AS (PARTITION BY opened_at_dayofweek), w_all_by_opened_at AS (ORDER BY opened_at, id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        csat,
        tags,
        opened_at_dayofweek,
        greatest_csat_requester_id,
        opened_at_dayofweek_agent_id_min_csat,
        csat_avg_3,
        opened_at_dayofweek_avg_3_prior,
        distinct_channel_in_opened_at_dayofweek_agent_id,
        greatest_csat_requester_id_share_of_opened_at_da,
        greatest_csat_requester_id_change,
        opened_at_dayofweek_share_of_opened_at_dayofweek,
        prev2_opened_at,
        greatest_csat_requester_id_quintile,
        opened_at_tercile,
        last_csat,
        greatest_csat_requester_id_tercile,
        csat_percent_rank
    FROM metrics
    WHERE csat_dense_rank = 1
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        channel,
        tags,
        opened_at_dayofweek_agent_id_min_csat,
        csat_avg_3,
        distinct_channel_in_opened_at_dayofweek_agent_id,
        greatest_csat_requester_id_share_of_opened_at_da,
        opened_at_dayofweek_share_of_opened_at_dayofweek,
        opened_at_tercile,
        last_csat,
        FIRST_VALUE(last_csat) OVER (PARTITION BY TO_DATE(opened_at) ORDER BY greatest_csat_requester_id_quintile, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_last_csat,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), greatest_csat_requester_id_tercile ORDER BY prev2_opened_at NULLS LAST) AS prev2_opened_at_rank,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY last_csat, distinct_channel_in_opened_at_dayofweek_agent_id), 0), 4) AS csat_share_of_last_csat_distinct_channel_in_open,
        LAST_VALUE(prev2_opened_at) IGNORE NULLS OVER (PARTITION BY greatest_csat_requester_id_quintile, TO_DATE(opened_at) ORDER BY csat_percent_rank DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev2_opened_at,
        LAST_VALUE(csat_avg_3) OVER (PARTITION BY TO_DATE(opened_at) ORDER BY greatest_csat_requester_id_quintile, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat_avg_3,
        FIRST_VALUE(opened_at_dayofweek) OVER (PARTITION BY TO_DATE(opened_at) ORDER BY greatest_csat_requester_id_quintile, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_at_dayofweek,
        ROUND(greatest_csat_requester_id - AVG(greatest_csat_requester_id) OVER (PARTITION BY last_csat, distinct_channel_in_opened_at_dayofweek_agent_id), 2) AS greatest_csat_requester_id_vs_last_csat_distinct
    FROM top_per_group
)
-- Final projection
SELECT
    id,
    channel,
    tags,
    opened_at_dayofweek_agent_id_min_csat,
    greatest_csat_requester_id_share_of_opened_at_da,
    opened_at_dayofweek_share_of_opened_at_dayofweek,
    opened_at_tercile,
    last_csat,
    first_last_csat,
    prev2_opened_at_rank,
    first_opened_at_dayofweek,
    greatest_csat_requester_id_vs_last_csat_distinct
FROM metrics_2
ORDER BY first_last_csat, id
LIMIT 5
