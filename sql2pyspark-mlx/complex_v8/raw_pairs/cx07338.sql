WITH base_tickets AS (
    -- Filter tickets
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
        requester_id + csat AS requester_id_plus_csat,
        COALESCE(csat, 0) AS csat_filled
    FROM tickets
    WHERE SIZE(tags) >= 2
      AND priority NOT IN ('P4', 'P2')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        requester_id_plus_csat,
        NTILE(3) OVER (ORDER BY requester_id ASC, id) AS requester_id_tercile,
        LEAD(csat, 2) OVER (ORDER BY requester_id ASC, id) AS next2_csat,
        SUM(requester_id) OVER w_closed_at_to_date_priority AS closed_at_to_date_priority_sum_requester_id,
        ROUND(AVG(csat) OVER (PARTITION BY TO_DATE(closed_at), priority), 2) AS closed_at_to_date_priority_avg_csat,
        ROUND(AVG(csat_filled) OVER (PARTITION BY agent_id ORDER BY csat_filled ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS csat_filled_avg_7,
        ROUND(CUME_DIST() OVER (ORDER BY requester_id ASC), 3) AS requester_id_cume_dist,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY TO_DATE(closed_at), priority)) AS distinct_channel_in_closed_at_to_date_priority,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY TO_DATE(closed_at), priority)) AS distinct_agent_id_in_closed_at_to_date_priority,
        ROW_NUMBER() OVER w_csat_by_requester_id_plus_cs AS seq_no,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY csat ORDER BY requester_id_plus_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id,
        ROW_NUMBER() OVER (ORDER BY requester_id ASC, id) AS requester_id_seq,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY csat ORDER BY requester_id_plus_csat DESC, id))) / 60, 1) AS minutes_since_prev_opened_at,
        ROUND(csat_filled - AVG(csat_filled) OVER w_closed_at_to_date_priority, 2) AS csat_filled_vs_closed_at_to_date_priority_avg
    FROM base_tickets
    WINDOW w_closed_at_to_date_priority AS (PARTITION BY TO_DATE(closed_at), priority), w_csat_by_requester_id_plus_cs AS (PARTITION BY csat ORDER BY requester_id_plus_csat DESC, id)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        seq_no,
        COUNT(*) AS row_count,
        SUM(CASE WHEN priority IN ('P2', 'P1', 'P3') THEN 1 ELSE 0 END) AS n_priority_in_p2_p1_p3
    FROM ranked
    GROUP BY seq_no
)
SELECT
    seq_no,
    row_count,
    n_priority_in_p2_p1_p3,
    GREATEST(seq_no, row_count) AS greatest_seq_no_row_count
FROM summary
ORDER BY row_count DESC, greatest_seq_no_row_count NULLS LAST
