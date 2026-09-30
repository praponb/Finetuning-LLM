WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('system', 'customer') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group,
        CONCAT_WS('|', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE event_type = 'assigned'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        event_type,
        actor,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        COUNT(*) AS cnt,
        SUM(CASE WHEN actor IN ('customer', 'system') THEN 1 ELSE 0 END) AS n_actor_in_customer_system,
        COUNT(*) FILTER (WHERE actor <> 'system') AS n_actor_system,
        COUNT(DISTINCT actor) AS distinct_actors,
        MAX(event_at) AS last_event_at
    FROM cleaned
    GROUP BY event_type, actor
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        event_type,
        actor,
        actor_list,
        cnt,
        n_actor_in_customer_system,
        n_actor_system,
        last_event_at,
        NTILE(5) OVER w_all_by_last_event_at AS last_event_at_quintile,
        MIN(distinct_actors) OVER (PARTITION BY distinct_actors) AS distinct_actors_min_distinct_actors,
        LAG(last_event_at, 3) OVER (ORDER BY n_actor_system DESC, event_type, actor) AS prev3_last_event_at,
        ROUND(n_actor_system - AVG(n_actor_system) OVER (PARTITION BY distinct_actors), 2) AS n_actor_system_vs_distinct_actors_avg,
        SIZE(COLLECT_SET(actor) OVER w_distinct_actors) AS distinct_actor_in_distinct_actors
    FROM stats
    WINDOW w_all_by_last_event_at AS (ORDER BY last_event_at NULLS LAST, event_type, actor), w_distinct_actors AS (PARTITION BY distinct_actors)
),
exploded AS (
    -- One row per actor_list_item
    SELECT
        s.event_type,
        s.actor,
        s.cnt,
        s.n_actor_in_customer_system,
        s.n_actor_system,
        s.last_event_at,
        s.last_event_at_quintile,
        s.distinct_actors_min_distinct_actors,
        s.prev3_last_event_at,
        s.n_actor_system_vs_distinct_actors_avg,
        s.distinct_actor_in_distinct_actors,
        t.actor_list_item_pos,
        t.actor_list_item
    FROM with_windows s
    LATERAL VIEW POSEXPLODE(s.actor_list) t AS actor_list_item_pos, actor_list_item
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM exploded
    WHERE distinct_actors_min_distinct_actors > (SELECT AVG(distinct_actors_min_distinct_actors) FROM exploded)
)
SELECT
    event_type,
    actor,
    cnt,
    n_actor_in_customer_system,
    n_actor_system,
    last_event_at,
    last_event_at_quintile,
    distinct_actors_min_distinct_actors,
    prev3_last_event_at,
    n_actor_system_vs_distinct_actors_avg,
    actor_list_item,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
    ABS(n_actor_system - 3) AS n_actor_system_distance
FROM screened
ORDER BY cnt DESC
