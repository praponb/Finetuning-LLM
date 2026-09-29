WITH scoped AS (
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
        tags
    FROM tickets
    WHERE channel IN ('web', 'email')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        SIZE(COLLECT_SET(agent_id) OVER w_csat) AS distinct_agent_id_in_csat,
        MIN(csat) OVER w_agent_id AS agent_id_min_csat,
        ROW_NUMBER() OVER (PARTITION BY channel, YEAR(opened_at) ORDER BY requester_id DESC, id) AS rn,
        ROUND(CUME_DIST() OVER w_agent_id_by_requester_id, 3) AS requester_id_cume_dist,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        SUM(requester_id) OVER (PARTITION BY csat) AS csat_sum_requester_id,
        SIZE(COLLECT_SET(channel) OVER w_closed_at_to_date) AS distinct_channel_in_closed_at_to_date,
        NTILE(3) OVER (ORDER BY csat, id) AS csat_tercile,
        NTILE(4) OVER w_csat_opened_at_month_by_requ AS requester_id_quartile,
        COUNT(*) OVER w_closed_at_to_date AS rows_in_closed_at_to_date,
        LAST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY requester_id NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_csat,
        COUNT(*) OVER w_csat AS rows_in_csat,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY agent_id, priority ORDER BY requester_id, id))) / 60, 1) AS minutes_since_prev_opened_at
    FROM scoped
    WINDOW w_csat AS (PARTITION BY csat), w_agent_id AS (PARTITION BY agent_id), w_agent_id_by_requester_id AS (PARTITION BY agent_id ORDER BY requester_id NULLS LAST, id), w_closed_at_to_date AS (PARTITION BY TO_DATE(closed_at)), w_csat_opened_at_month_by_requ AS (PARTITION BY csat, DATE_TRUNC('MONTH', opened_at) ORDER BY requester_id ASC, id)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    csat,
    distinct_agent_id_in_csat,
    agent_id_min_csat,
    rn,
    second_closed_at,
    csat_sum_requester_id,
    distinct_channel_in_closed_at_to_date,
    csat_tercile,
    requester_id_quartile,
    rows_in_csat,
    minutes_since_prev_opened_at,
    DATEDIFF(DATE '2026-01-01', opened_at) AS days_since_opened_at
FROM leaders
ORDER BY priority, distinct_channel_in_closed_at_to_date DESC NULLS FIRST, id
LIMIT 10
