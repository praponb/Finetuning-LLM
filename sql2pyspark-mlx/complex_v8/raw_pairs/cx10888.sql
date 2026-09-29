WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(actor)) AS actor_clean,
        UPPER(TRIM(event_type)) AS event_type_clean,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE actor <> 'agent'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        SUM(event_type_len) OVER (PARTITION BY event_type, event_type_len) AS event_type_event_type_len_sum_event_type_len,
        NTILE(10) OVER (ORDER BY event_at, id) AS event_at_decile,
        LAST_VALUE(event_type_len) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_type_len,
        NTILE(5) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id) AS event_at_quintile
    FROM scoped_events
),
latest_per_key AS (
    -- Keep the earliest row per actor
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        event_type_event_type_len_sum_event_type_len,
        event_at_decile,
        last_event_type_len,
        event_at_quintile
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor ORDER BY event_at_decile, id) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
),
rollup_base AS (
    -- Summarise per group
    SELECT
        event_at_decile,
        event_at_quintile,
        SUM(CASE WHEN actor = 'system' THEN 1 ELSE 0 END) AS n_actor_system,
        ROUND(STDDEV(last_event_type_len), 2) AS stddev_last_event_type_len,
        COUNT(*) AS row_count,
        COUNT(DISTINCT actor) AS distinct_actors,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        PERCENTILE_APPROX(event_at_decile, 0.5) AS median_event_at_decile
    FROM latest_per_key
    WHERE TO_DATE(event_at) < DATE '2025-02-06'
    GROUP BY event_at_decile, event_at_quintile
    HAVING COUNT(*) >= 2
),
labeled AS (
    -- Add calculated columns
    SELECT
        event_at_decile,
        event_at_quintile,
        n_actor_system,
        row_count,
        median_event_at_decile,
        CASE WHEN n_actor_system >= 8 THEN 'large' WHEN n_actor_system >= 2 THEN 'medium' ELSE 'small' END AS n_actor_system_tier
    FROM rollup_base
)
SELECT
    event_at_decile,
    event_at_quintile,
    median_event_at_decile,
    n_actor_system_tier,
    LEAST(row_count, n_actor_system) AS least_row_count_n_actor_system
FROM labeled
ORDER BY median_event_at_decile NULLS LAST
