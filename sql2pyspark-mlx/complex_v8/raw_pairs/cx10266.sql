WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE MONTH(event_at) IN (1, 2)
      AND event_type IN ('reopened', 'assigned', 'created')
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_month,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM cleaned_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.actor NOT IN ('agent', 'customer')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(event_event_at) OVER (PARTITION BY actor, ticket_id ORDER BY event_event_at DESC NULLS FIRST, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_event_at,
        LEAD(event_event_at) OVER (ORDER BY event_at_month DESC, id, event_id) AS next_event_event_at
    FROM exploded_detail
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        event_at_month,
        event_id,
        event_actor,
        last_event_event_at,
        next_event_event_at,
        CONCAT_WS('-', actor, event_actor) AS actor_event_actor_label,
        CASE WHEN event_actor IN ('customer', 'agent') THEN 'group_a' WHEN event_actor = 'system' THEN 'group_b' ELSE 'other' END AS event_actor_group,
        SUBSTRING(TRIM(event_actor), 1, 2) AS event_actor_prefix
    FROM windowed
    WHERE actor IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY event_type, event_actor ORDER BY next_event_event_at, id, event_id) AS next_event_event_at_rank,
        DENSE_RANK() OVER (PARTITION BY actor, ticket_id ORDER BY last_event_event_at DESC, id, event_id) AS last_event_event_at_dense_rank,
        LEAD(event_at_month, 2) OVER (PARTITION BY event_type, event_actor ORDER BY next_event_event_at, id, event_id) AS next2_event_at_month
    FROM labeled
),
per_group AS (
    -- Group-level totals
    SELECT
        last_event_event_at_dense_rank,
        event_actor_group,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT event_actor_group) AS distinct_event_actor_groups,
        MAX(last_event_event_at_dense_rank) AS max_last_event_event_at_dense_rank,
        ROUND(STDDEV(next_event_event_at_rank), 2) AS stddev_next_event_event_at_rank,
        ROUND(100 * COUNT(CASE WHEN actor IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_actor_is_not_null,
        SUM(last_event_event_at_dense_rank) AS total_last_event_event_at_dense_rank
    FROM ranked
    WHERE MONTH(event_at_month) IN (2, 3)
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY last_event_event_at_dense_rank ORDER BY total_last_event_event_at_dense_rank ASC, last_event_event_at_dense_rank, event_actor_group) AS total_last_event_event_at_dense_rank_quintile,
        NTH_VALUE(pct_actor_is_not_null, 2) OVER (PARTITION BY last_event_event_at_dense_rank ORDER BY total_last_event_event_at_dense_rank ASC, last_event_event_at_dense_rank, event_actor_group ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_pct_actor_is_not_null,
        MAX(n_rows) OVER (PARTITION BY distinct_event_actor_groups) AS distinct_event_actor_groups_max_n_rows,
        ROW_NUMBER() OVER w_event_actor_group_last_event AS seq_no,
        ROUND(PERCENT_RANK() OVER (PARTITION BY last_event_event_at_dense_rank ORDER BY total_last_event_event_at_dense_rank ASC), 3) AS total_last_event_event_at_dense_rank_percent_ran
    FROM per_group
    WINDOW w_event_actor_group_last_event AS (PARTITION BY event_actor_group, last_event_event_at_dense_rank ORDER BY distinct_event_actor_groups, last_event_event_at_dense_rank, event_actor_group)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT n_rows, event_actor_group, max_last_event_event_at_dense_rank
    FROM with_trends
) src
PIVOT (
    COUNT(max_last_event_event_at_dense_rank)
    FOR event_actor_group IN ('group_b', 'other')
)
ORDER BY n_rows
