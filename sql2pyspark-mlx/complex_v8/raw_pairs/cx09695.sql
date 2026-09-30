WITH valid AS (
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
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 3600, 1) AS hours_closed_at_to_opened_at,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        closed_at + INTERVAL 2 HOURS AS closed_at_plus_2h
    FROM tickets
    WHERE channel = 'phone'
),
deduped AS (
    -- Keep the latest row per agent_id
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
        hours_closed_at_to_opened_at,
        opened_at_month,
        closed_at_plus_2h
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY opened_at_month DESC, id DESC) AS row_num
        FROM valid
    ) ranked
    WHERE row_num = 1
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        opened_at,
        priority,
        hours_closed_at_to_opened_at,
        ROW_NUMBER() OVER (ORDER BY closed_at_plus_2h NULLS LAST, id) AS closed_at_plus_2h_seq,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY csat, priority), 2) AS requester_id_vs_csat_priority_avg,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY csat, priority)) AS distinct_channel_in_csat_priority
    FROM deduped
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        opened_at,
        priority,
        hours_closed_at_to_opened_at,
        requester_id_vs_csat_priority_avg,
        ROW_NUMBER() OVER (PARTITION BY priority, closed_at_plus_2h_seq ORDER BY requester_id_vs_csat_priority_avg DESC, id) AS requester_id_vs_csat_priority_avg_seq,
        MAX(requester_id) OVER (PARTITION BY priority, closed_at_plus_2h_seq ORDER BY requester_id_vs_csat_priority_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_requester_id_so_far,
        requester_id - LAG(requester_id) OVER w_priority_closed_at_plus_2h_s AS requester_id_change,
        LAST_VALUE(closed_at_plus_2h_seq) OVER (PARTITION BY priority, closed_at_plus_2h_seq ORDER BY requester_id_vs_csat_priority_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at_plus_2h_seq
    FROM with_windows
    WINDOW w_priority_closed_at_plus_2h_s AS (PARTITION BY priority, closed_at_plus_2h_seq ORDER BY requester_id_vs_csat_priority_avg DESC, id)
)
SELECT
    requester_id_vs_csat_priority_avg_seq,
    COALESCE(priority, 'ALL') AS priority,
    COUNT(*) AS cnt,
    MIN(opened_at) AS first_opened_at,
    ROUND(100 * COUNT(CASE WHEN priority <> 'P3' THEN 1 END) / COUNT(*), 1) AS pct_priority_p3
FROM with_windows_2
GROUP BY GROUPING SETS ((requester_id_vs_csat_priority_avg_seq, priority), (requester_id_vs_csat_priority_avg_seq), (priority), ())
