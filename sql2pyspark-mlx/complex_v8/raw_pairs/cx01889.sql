WITH matched AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
latest AS (
    -- Keep the earliest row per ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY hours_since_open, id) AS rn
        FROM matched
    ) ranked
    WHERE rn = 1
),
per_group AS (
    -- Group-level totals
    SELECT
        event_type,
        priority,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        SUM(CASE WHEN priority IS NOT NULL THEN 1 ELSE 0 END) AS n_priority_is_not_null,
        SUM(CASE WHEN priority <> 'P4' THEN hours_since_open ELSE 0 END) AS hours_since_open_priority_p4,
        MAX(hours_since_open) AS max_hours_since_open
    FROM latest
    WHERE hours_since_open BETWEEN 10.8 AND 57.6
    GROUP BY 1, 2
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY n_priority_is_not_null, event_type) AS rows_in_n_priority_is_not_null_event_type,
        ROUND(n_priority_is_not_null - AVG(n_priority_is_not_null) OVER (PARTITION BY n_priority_is_not_null, event_type), 2) AS n_priority_is_not_null_vs_n_priority_is_not_null,
        NTILE(3) OVER (ORDER BY hours_since_open_priority_p4 NULLS LAST, event_type, priority) AS hours_since_open_priority_p4_tercile,
        hours_since_open_priority_p4 - LAG(hours_since_open_priority_p4) OVER w_all_by_max_hours_since_open AS hours_since_open_priority_p4_change,
        COUNT(*) OVER (ORDER BY max_hours_since_open ASC, event_type, priority ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM per_group
    WINDOW w_all_by_max_hours_since_open AS (ORDER BY max_hours_since_open ASC, event_type, priority)
)
SELECT
    event_type,
    priority,
    event_type_list,
    hours_since_open_priority_p4,
    max_hours_since_open,
    rows_in_n_priority_is_not_null_event_type,
    n_priority_is_not_null_vs_n_priority_is_not_null,
    hours_since_open_priority_p4_tercile,
    hours_since_open_priority_p4_change,
    running_count,
    max_hours_since_open - cnt AS max_hours_since_open_minus_cnt,
    CONCAT(event_type, '#', CAST(cnt AS STRING)) AS event_type_ref
FROM metrics
ORDER BY hours_since_open_priority_p4_tercile, event_type, priority
LIMIT 5
