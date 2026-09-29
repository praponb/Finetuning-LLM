WITH valid_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        LOWER(TRIM(priority)) AS priority_clean
    FROM tickets
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        priority_clean,
        ROUND(AVG(requester_id) OVER (PARTITION BY priority), 2) AS priority_avg_requester_id,
        SUM(requester_id) OVER (PARTITION BY TO_DATE(closed_at)) AS closed_at_to_date_sum_requester_id,
        SUM(requester_id) OVER (PARTITION BY priority) AS priority_sum_requester_id,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY YEAR(closed_at), agent_id ORDER BY requester_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        ROUND(AVG(requester_id) OVER (PARTITION BY channel, agent_id ORDER BY requester_id DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_2,
        RANK() OVER (PARTITION BY channel ORDER BY closed_at DESC, id) AS closed_at_rank,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY channel ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at,
        SUM(requester_id) OVER (PARTITION BY YEAR(closed_at), agent_id ORDER BY requester_id ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS requester_id_sum_2,
        requester_id - LAG(requester_id) OVER (PARTITION BY channel ORDER BY closed_at DESC, id) AS requester_id_change
    FROM valid_tickets
),
leaders AS (
    -- First row per group
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        priority_clean,
        priority_avg_requester_id,
        closed_at_to_date_sum_requester_id,
        priority_sum_requester_id,
        second_closed_at,
        requester_id_avg_2,
        first_known_closed_at,
        requester_id_sum_2,
        requester_id_change
    FROM sequenced
    WHERE closed_at_rank = 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        priority_clean,
        priority_avg_requester_id,
        closed_at_to_date_sum_requester_id,
        priority_sum_requester_id,
        second_closed_at,
        first_known_closed_at,
        requester_id_sum_2,
        requester_id_change,
        ROUND(CUME_DIST() OVER (PARTITION BY priority ORDER BY requester_id_sum_2, id), 3) AS requester_id_sum_2_cume_dist,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (PARTITION BY agent_id ORDER BY priority_sum_requester_id DESC, id))) / 60, 1) AS minutes_since_prev_closed_at,
        SIZE(COLLECT_SET(agent_id) OVER w_first_known_closed_at_month) AS distinct_agent_id_in_first_known_closed_at_month,
        MAX(requester_id_avg_2) OVER (PARTITION BY TO_DATE(second_closed_at), TO_DATE(closed_at) ORDER BY requester_id ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS requester_id_avg_2_max_7,
        LAST_VALUE(first_known_closed_at) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY priority_sum_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_known_closed_at,
        COUNT(*) OVER w_first_known_closed_at_month AS rows_in_first_known_closed_at_month_priority,
        LAG(requester_id_sum_2, 2, 0) OVER (PARTITION BY TO_DATE(second_closed_at), TO_DATE(closed_at) ORDER BY requester_id ASC, id) AS prev2_requester_id_sum_2
    FROM leaders
    WINDOW w_first_known_closed_at_month AS (PARTITION BY MONTH(first_known_closed_at), priority)
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    priority,
    channel,
    priority_clean,
    priority_avg_requester_id,
    closed_at_to_date_sum_requester_id,
    priority_sum_requester_id,
    requester_id_sum_2,
    minutes_since_prev_closed_at,
    distinct_agent_id_in_first_known_closed_at_month,
    last_known_first_known_closed_at,
    rows_in_first_known_closed_at_month_priority,
    prev2_requester_id_sum_2,
    SUBSTRING(TRIM(priority), 1, 4) AS priority_prefix,
    CONCAT(channel, '#', CAST(requester_id AS STRING)) AS channel_ref
FROM windowed
ORDER BY requester_id DESC NULLS FIRST
