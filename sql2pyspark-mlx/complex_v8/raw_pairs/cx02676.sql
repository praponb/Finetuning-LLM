WITH base_tickets AS (
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
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'outage')
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(requester_id) OVER (PARTITION BY priority, TO_DATE(opened_at)) AS priority_opened_at_to_date_max_requester_id,
        RANK() OVER (ORDER BY requester_id) AS requester_id_rank,
        LAST_VALUE(csat) IGNORE NULLS OVER (ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_csat,
        ROW_NUMBER() OVER w_opened_at_to_date_channel_by AS rn,
        SUM(csat) OVER (ORDER BY requester_id, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS csat_sum_3,
        SUM(requester_id) OVER (ORDER BY opened_at ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS requester_id_sum_6_prior,
        NTILE(3) OVER (PARTITION BY MONTH(closed_at), channel ORDER BY csat DESC, id) AS csat_tercile,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY priority, TO_DATE(opened_at))) AS distinct_agent_id_in_priority_opened_at_to_date,
        NTH_VALUE(requester_id, 2) OVER (ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        FIRST_VALUE(csat) OVER (PARTITION BY MONTH(closed_at), channel ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER w_agent_id_priority_by_opened)) / 60, 1) AS minutes_since_prev_opened_at,
        ROUND(AVG(requester_id) OVER w_agent_id_closed_at_year, 2) AS agent_id_closed_at_year_avg_requester_id
    FROM base_tickets
    WINDOW w_opened_at_to_date_channel_by AS (PARTITION BY TO_DATE(opened_at), channel ORDER BY opened_at, id), w_agent_id_priority_by_opened AS (PARTITION BY agent_id, priority ORDER BY opened_at, id), w_agent_id_closed_at_year AS (PARTITION BY agent_id, YEAR(closed_at))
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        opened_at,
        closed_at,
        priority,
        tags,
        last_known_csat,
        rn,
        requester_id_sum_6_prior,
        distinct_agent_id_in_priority_opened_at_to_date,
        minutes_since_prev_opened_at,
        agent_id_closed_at_year_avg_requester_id,
        COUNT(*) OVER (PARTITION BY first_csat, channel) AS rows_in_first_csat_channel,
        MAX(csat) OVER (PARTITION BY first_csat, channel) AS first_csat_channel_max_csat,
        NTILE(3) OVER (PARTITION BY first_csat ORDER BY last_known_csat ASC, id) AS last_known_csat_tercile,
        ROUND(AVG(requester_id_sum_6_prior) OVER (ORDER BY last_known_csat, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS requester_id_sum_6_prior_avg_6_prior,
        LAG(minutes_since_prev_opened_at) OVER (PARTITION BY first_csat ORDER BY last_known_csat ASC, id) AS prev_minutes_since_prev_opened_at,
        NTILE(3) OVER (ORDER BY last_known_csat, id) AS last_known_csat_tercile_2
    FROM sequenced
)
-- Final projection
SELECT
    id,
    requester_id,
    opened_at,
    closed_at,
    tags,
    last_known_csat,
    rn,
    requester_id_sum_6_prior,
    distinct_agent_id_in_priority_opened_at_to_date,
    minutes_since_prev_opened_at,
    agent_id_closed_at_year_avg_requester_id,
    requester_id_sum_6_prior_avg_6_prior,
    prev_minutes_since_prev_opened_at,
    last_known_csat_tercile_2,
    DATE_FORMAT(closed_at, 'yyyy-MM') AS closed_at_label,
    QUARTER(opened_at) AS opened_at_quarter
FROM with_trends
WHERE agent_id_closed_at_year_avg_requester_id > 11.8
ORDER BY last_known_csat NULLS LAST, id
LIMIT 20
