WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        DATEDIFF(DATE '2025-06-30', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE channel IN ('phone', 'chat', 'web')
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(channel)) AS channel_len,
        CASE WHEN csat >= 3 THEN 'A' WHEN csat >= 2 THEN 'B' ELSE 'C' END AS csat_tier
    FROM recent_tickets
),
derived_2 AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(days_since_opened_at / NULLIF(csat, 0), 3) AS days_since_opened_at_per_csat
    FROM derived
    WHERE csat_tier IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(requester_id) OVER (ORDER BY days_since_opened_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS requester_id_sum_6_prior,
        ROUND(CUME_DIST() OVER (ORDER BY days_since_opened_at DESC), 3) AS days_since_opened_at_cume_dist,
        NTILE(3) OVER w_all_by_days_since_opened_at_3 AS days_since_opened_at_tercile,
        ROUND(csat - AVG(csat) OVER (PARTITION BY csat, agent_id), 2) AS csat_vs_csat_agent_id_avg,
        ROUND(PERCENT_RANK() OVER w_all_by_days_since_opened_at_3, 3) AS days_since_opened_at_percent_rank,
        ROUND(AVG(csat) OVER (PARTITION BY csat, agent_id), 2) AS csat_agent_id_avg_csat
    FROM derived_2
    WINDOW w_all_by_days_since_opened_at_3 AS (ORDER BY days_since_opened_at DESC, id)
),
joined_agent AS (
    -- Join support_agents
    SELECT s.*, su.hired_on AS agent_hired_on
    FROM windowed s
    JOIN support_agents su ON su.id = s.agent_id
),
grouped AS (
    -- Summarise per group
    SELECT
        channel_len,
        ROUND(SUM(csat_agent_id_avg_csat) / NULLIF(SUM(requester_id), 0), 3) AS csat_agent_id_avg_csat_to_requester_id_ratio,
        ROUND(AVG(requester_id_sum_6_prior), 2) AS avg_requester_id_sum_6_prior,
        ROUND(SUM(days_since_opened_at_percent_rank), 2) AS total_days_since_opened_at_percent_rank,
        SUM(days_since_opened_at_tercile) AS total_days_since_opened_at_tercile
    FROM joined_agent
    GROUP BY 1
)
-- Final projection
SELECT
    channel_len,
    csat_agent_id_avg_csat_to_requester_id_ratio,
    avg_requester_id_sum_6_prior,
    total_days_since_opened_at_percent_rank,
    COALESCE(channel_len, 0) AS channel_len_filled,
    CASE WHEN channel_len >= 9 THEN 'A' WHEN channel_len >= 6 THEN 'B' ELSE 'C' END AS channel_len_tier
FROM grouped
ORDER BY channel_len_tier, channel_len_filled DESC, channel_len
LIMIT 10
