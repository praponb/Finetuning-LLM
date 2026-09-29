WITH filtered_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        CONCAT_WS('-', priority, channel) AS priority_channel_label
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-19 07:00:00'
),
latest_per_key AS (
    -- Keep the latest row per priority
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        priority_slug,
        priority_channel_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority ORDER BY opened_at DESC, id DESC) AS rn
        FROM filtered_tickets
    ) ranked
    WHERE rn = 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority_slug,
        SIZE(COLLECT_SET(channel) OVER w_agent_id) AS distinct_channel_in_agent_id,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id, priority ORDER BY requester_id DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_4,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (ORDER BY requester_id DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_opened_at,
        MAX(requester_id) OVER (PARTITION BY channel ORDER BY requester_id DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS requester_id_max_6_prior,
        RANK() OVER (PARTITION BY agent_id, priority ORDER BY requester_id DESC, id) AS requester_id_rank,
        NTILE(5) OVER (PARTITION BY agent_id, priority ORDER BY requester_id DESC, id) AS requester_id_quintile
    FROM latest_per_key
    WINDOW w_agent_id AS (PARTITION BY agent_id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE requester_id_rank = 1
      AND agent_id IS NOT NULL
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_channel_in_agent_id,
        requester_id_quintile,
        MAX(opened_at) AS last_opened_at,
        ROUND(SUM(requester_id_avg_4) / NULLIF(SUM(requester_id), 0), 3) AS requester_id_avg_4_to_requester_id_ratio,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(requester_id_quintile) AS total_requester_id_quintile
    FROM first_rows
    GROUP BY distinct_channel_in_agent_id, requester_id_quintile
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('HOUR', last_opened_at) AS last_opened_at_hour,
        DAYOFWEEK(last_opened_at) AS last_opened_at_dayofweek,
        FLOOR(requester_id_quintile / 2) * 2 AS requester_id_quintile_bucket
    FROM rollup_base
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM derived
    WHERE row_count > (SELECT MIN(row_count) FROM derived)
)
-- Final projection
SELECT
    distinct_channel_in_agent_id,
    requester_id_quintile,
    last_opened_at,
    row_count,
    last_opened_at_hour,
    last_opened_at_dayofweek,
    requester_id_quintile_bucket,
    DATEDIFF(DATE '2025-12-31', last_opened_at_hour) AS days_since_last_opened_at_hour,
    DATE_TRUNC('MONTH', last_opened_at_hour) AS last_opened_at_hour_month
FROM filtered
WHERE last_opened_at < TIMESTAMP '2025-02-12 10:00:00'
ORDER BY days_since_last_opened_at_hour
