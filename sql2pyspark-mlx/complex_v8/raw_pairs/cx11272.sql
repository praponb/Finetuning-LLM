WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE csat < 5
      AND closed_at < TIMESTAMP '2025-02-19 21:00:00'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        tags,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY TO_DATE(opened_at)), 0), 4) AS requester_id_share_of_opened_at_to_date,
        ROUND(csat - AVG(csat) OVER (PARTITION BY TO_DATE(opened_at)), 2) AS csat_vs_opened_at_to_date_avg,
        MIN(csat) OVER (PARTITION BY TO_DATE(opened_at)) AS opened_at_to_date_min_csat,
        MIN(requester_id) OVER (PARTITION BY TO_DATE(opened_at)) AS opened_at_to_date_min_requester_id,
        DENSE_RANK() OVER (PARTITION BY agent_id, TO_DATE(opened_at) ORDER BY requester_id) AS requester_id_dense_rank
    FROM filtered_tickets
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT MIN(t.requester_id) FROM tickets t WHERE t.agent_id = s.agent_id) AS min_ticket_requester_id
    FROM with_windows s
),
with_windows_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(requester_id_dense_rank) OVER (PARTITION BY ticket_count ORDER BY csat_vs_opened_at_to_date_avg ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id_dense_rank,
        RANK() OVER (PARTITION BY ticket_count ORDER BY csat_vs_opened_at_to_date_avg ASC) AS csat_vs_opened_at_to_date_avg_rank,
        RANK() OVER (PARTITION BY requester_id_dense_rank ORDER BY requester_id_share_of_opened_at_to_date, id) AS requester_id_share_of_opened_at_to_date_rank,
        ROW_NUMBER() OVER (PARTITION BY ticket_count ORDER BY csat_vs_opened_at_to_date_avg ASC, id) AS csat_vs_opened_at_to_date_avg_seq,
        NTILE(3) OVER w_ticket_count_by_csat_vs_open_3 AS csat_vs_opened_at_to_date_avg_tercile
    FROM with_stats
    WINDOW w_ticket_count_by_csat_vs_open_3 AS (PARTITION BY ticket_count ORDER BY csat_vs_opened_at_to_date_avg ASC, id)
)
SELECT
    channel,
    csat_vs_opened_at_to_date_avg_tercile,
    priority,
    GROUPING(channel) AS is_total_channel,
    COUNT(*) AS cnt,
    SUM(opened_at_to_date_min_requester_id) AS total_opened_at_to_date_min_requester_id
FROM with_windows_2
GROUP BY CUBE(channel, csat_vs_opened_at_to_date_avg_tercile, priority)
