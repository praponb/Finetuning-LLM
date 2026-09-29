WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-18 09:00:00'
),
ranked AS (
    -- Previous / next values per key
    SELECT id, priority, RANK() OVER (ORDER BY closed_at ASC, id) AS closed_at_rank
    FROM scoped
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        t.id AS event_id,
        t.event_at,
        t.event_type,
        t.actor AS event_actor
    FROM ranked s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        event_id,
        event_type,
        event_actor,
        COUNT(*) AS cnt,
        COUNT(DISTINCT event_id) AS distinct_event_ids,
        ROUND(100 * COUNT(CASE WHEN event_actor IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_event_actor_is_not_null,
        SUM(CASE WHEN event_type IN ('reopened', 'created', 'assigned') THEN 1 ELSE 0 END) AS n_event_type_in_reopened_creat,
        ARRAY_SORT(COLLECT_SET(event_actor)) AS event_actor_list
    FROM with_children
    GROUP BY 1, 2, 3
),
tagged AS (
    -- Label and score rows
    SELECT *, FLOOR(distinct_event_ids / 2) * 2 AS distinct_event_ids_bucket
    FROM grouped
    WHERE pct_event_actor_is_not_null < 60.0
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(pct_event_actor_is_not_null, 2) OVER (ORDER BY n_event_type_in_reopened_creat ASC, event_id, event_type, event_actor ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_pct_event_actor_is_not_null,
        LEAD(distinct_event_ids, 1, 0) OVER w_all_by_n_event_type_in_reope_2 AS next_distinct_event_ids,
        distinct_event_ids_bucket - LAG(distinct_event_ids_bucket) OVER w_all_by_n_event_type_in_reope_2 AS distinct_event_ids_bucket_change,
        ROUND(PERCENT_RANK() OVER (ORDER BY n_event_type_in_reopened_creat ASC), 3) AS n_event_type_in_reopened_creat_percent_rank,
        COUNT(*) OVER (ORDER BY n_event_type_in_reopened_creat ASC, event_id, event_type, event_actor ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM tagged
    WINDOW w_all_by_n_event_type_in_reope_2 AS (ORDER BY n_event_type_in_reopened_creat ASC, event_id, event_type, event_actor)
)
-- Final projection
SELECT
    event_id,
    event_type,
    event_actor,
    distinct_event_ids,
    n_event_type_in_reopened_creat,
    event_actor_list,
    distinct_event_ids_bucket,
    second_pct_event_actor_is_not_null,
    next_distinct_event_ids,
    distinct_event_ids_bucket_change,
    n_event_type_in_reopened_creat_percent_rank,
    running_count,
    SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix,
    SUBSTRING(TRIM(event_actor), 1, 2) AS event_actor_prefix
FROM windowed
ORDER BY distinct_event_ids_bucket DESC
