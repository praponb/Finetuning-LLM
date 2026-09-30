WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text,
        LENGTH(TRIM(priority)) AS priority_len
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 9 AND 11
      AND requester_id > 7
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        opened_at,
        csat,
        tags_text,
        priority_len,
        SUM(csat) OVER (PARTITION BY agent_id, DATE_TRUNC('MONTH', closed_at)) AS agent_id_closed_at_month_sum_csat,
        LAG(csat) OVER w_priority_len_priority_by_pri AS prev_csat,
        NTH_VALUE(csat, 2) OVER (PARTITION BY agent_id, MONTH(opened_at) ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        LEAD(requester_id) OVER (PARTITION BY priority ORDER BY csat DESC, id) AS next_requester_id,
        LAST_VALUE(opened_at) OVER (PARTITION BY agent_id, MONTH(opened_at) ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority ORDER BY csat DESC, id), 3) AS csat_percent_rank,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_id, DATE_TRUNC('MONTH', closed_at)), 0), 4) AS requester_id_share_of_agent_id_closed_at_month,
        NTILE(10) OVER (PARTITION BY priority_len, priority ORDER BY priority_len DESC, id) AS priority_len_decile,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY priority_len)) AS distinct_priority_in_priority_len,
        COUNT(*) OVER w_agent_id_closed_at_month AS rows_in_agent_id_closed_at_month,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY agent_id, DATE_TRUNC('MONTH', closed_at))) AS distinct_priority_in_agent_id_closed_at_month,
        MAX(priority_len) OVER (PARTITION BY priority_len) AS priority_len_max_priority_len
    FROM prepared_tickets
    WINDOW w_priority_len_priority_by_pri AS (PARTITION BY priority_len, priority ORDER BY priority_len DESC, id), w_agent_id_closed_at_month AS (PARTITION BY agent_id, DATE_TRUNC('MONTH', closed_at))
)
-- Final projection
SELECT
    id,
    csat,
    tags_text,
    priority_len,
    agent_id_closed_at_month_sum_csat,
    prev_csat,
    next_requester_id,
    last_opened_at,
    csat_percent_rank,
    priority_len_decile,
    rows_in_agent_id_closed_at_month
FROM with_trends
ORDER BY id DESC NULLS FIRST, next_requester_id NULLS LAST
