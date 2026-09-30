WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix
    FROM ticket_events
    WHERE actor <> 'agent'
      AND event_type = 'reopened'
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN actor IN ('system', 'customer') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group,
        CASE WHEN actor IN ('agent', 'customer') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group_2,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label
    FROM prepared
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY event_at, id) AS event_at_dense_rank,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        NTILE(3) OVER w_all_by_event_at AS event_at_tercile,
        RANK() OVER (ORDER BY event_at) AS event_at_rank
    FROM derived
    WINDOW w_all_by_event_at AS (ORDER BY event_at, id)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        event_at,
        actor,
        actor_prefix,
        actor_group,
        actor_group_2,
        event_at_label,
        event_at_tercile,
        event_at_rank,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix_2
    FROM with_trends
),
with_trends_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(event_at_rank - AVG(event_at_rank) OVER (PARTITION BY event_at_rank), 2) AS event_at_rank_vs_event_at_rank_avg,
        ROUND(PERCENT_RANK() OVER w_all_by_event_at_ties, 3) AS event_at_percent_rank
    FROM scored
    WINDOW w_all_by_event_at_ties AS (ORDER BY event_at)
)
SELECT
    actor,
    event_at_tercile,
    actor_group_2,
    ARRAY_SORT(COLLECT_SET(actor)) AS actor_list
FROM with_trends_2
GROUP BY GROUPING SETS ((actor, event_at_tercile, actor_group_2), (actor), (event_at_tercile))
ORDER BY actor NULLS LAST, event_at_tercile NULLS LAST, actor_group_2 NULLS LAST
