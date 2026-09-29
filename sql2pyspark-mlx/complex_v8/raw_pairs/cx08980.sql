WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        TO_DATE(opened_at) AS opened_at_date,
        GREATEST(csat, requester_id) AS greatest_csat_requester_id,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE csat <= 4
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.channel,
        s.opened_at_date,
        s.greatest_csat_requester_id,
        s.channel_len,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM recent_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(agent_tier) OVER w_agent_hired_on_year_channel) AS distinct_agent_tier_in_agent_hired_on_year_chann,
        MIN(requester_id) OVER w_agent_hired_on_year_channel AS agent_hired_on_year_channel_min_requester_id,
        ROUND(AVG(channel_len) OVER (ORDER BY greatest_csat_requester_id, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS channel_len_avg_2,
        ROUND(greatest_csat_requester_id - AVG(greatest_csat_requester_id) OVER (PARTITION BY YEAR(agent_hired_on), channel), 2) AS greatest_csat_requester_id_vs_agent_hired_on_yea,
        COUNT(*) OVER w_agent_hired_on_year_channel AS rows_in_agent_hired_on_year_channel
    FROM joined
    WINDOW w_agent_hired_on_year_channel AS (PARTITION BY YEAR(agent_hired_on), channel)
),
joined_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at_date,
        s.greatest_csat_requester_id,
        s.channel_len,
        s.agent_hired_on,
        s.agent_tier,
        s.agent_hired_on_year_channel_min_requester_id,
        s.rows_in_agent_hired_on_year_channel,
        su.hired_on AS agent_hired_on_2
    FROM with_windows s
    JOIN support_agents su ON su.id = s.agent_id
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', agent_hired_on_2) AS agent_hired_on_2_month,
        MAX(agent_hired_on_year_channel_min_requester_id) AS max_agent_hired_on_year_channel_min_requester_id,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE agent_tier = 'T3') AS n_agent_tier_t3
    FROM joined_agent
    GROUP BY 1
    HAVING MAX(rows_in_agent_hired_on_year_channel) > 8
),
derived AS (
    -- Label and score rows
    SELECT
        agent_hired_on_2_month,
        max_agent_hired_on_year_channel_min_requester_id,
        row_count,
        n_agent_tier_t3,
        ABS(row_count - 5) AS row_count_distance
    FROM stats
)
-- Native SQL UNPIVOT (wide to long)
SELECT agent_hired_on_2_month, metric, metric_value
FROM (
    SELECT agent_hired_on_2_month, row_count, n_agent_tier_t3, row_count_distance, max_agent_hired_on_year_channel_min_requester_id
    FROM derived
) src
UNPIVOT (
    metric_value FOR metric IN (row_count, n_agent_tier_t3, row_count_distance, max_agent_hired_on_year_channel_min_requester_id)
) unp
ORDER BY agent_hired_on_2_month, metric
