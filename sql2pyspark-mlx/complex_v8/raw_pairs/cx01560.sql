WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        actor,
        ticket_id,
        event_type,
        MAX(event_at) AS last_event_at,
        COUNT(*) FILTER (WHERE event_type NOT IN ('reopened', 'escalated')) AS n_event_type_not_in_reopened_e,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        ROUND(100 * COUNT(CASE WHEN event_type = 'created' THEN 1 END) / COUNT(*), 1) AS pct_event_type_created,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT actor) AS distinct_actors
    FROM cleaned
    GROUP BY 1, 2, 3
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        actor,
        ticket_id,
        event_type,
        last_event_at,
        pct_event_type_created,
        distinct_actors,
        SUM(pct_event_type_created) OVER (PARTITION BY distinct_ids) AS distinct_ids_sum_pct_event_type_created,
        RANK() OVER w_n_event_type_not_in_reopened AS distinct_ids_rank,
        ROW_NUMBER() OVER (ORDER BY last_event_at, actor, ticket_id, event_type) AS last_event_at_seq
    FROM summary
    WINDOW w_n_event_type_not_in_reopened AS (PARTITION BY n_event_type_not_in_reopened_e, distinct_actors ORDER BY distinct_ids DESC)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE last_event_at_seq = 1
      AND ticket_id >= 19
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix,
        CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group,
        CASE WHEN event_type IN ('created', 'escalated') THEN 'group_a' WHEN event_type = 'assigned' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM top_rows
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT distinct_ids_rank, event_type, actor_group, distinct_ids_sum_pct_event_type_created
    FROM derived
) src
PIVOT (
    SUM(distinct_ids_sum_pct_event_type_created) AS amount, MAX(distinct_ids_sum_pct_event_type_created) AS max_v
    FOR actor_group IN ('other', 'group_b', 'group_a')
)
ORDER BY distinct_ids_rank, event_type
