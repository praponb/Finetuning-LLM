WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IN ('resolved', 'replied')
      AND actor = 'agent'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        actor,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        ROUND(100 * COUNT(CASE WHEN event_type IN ('resolved', 'assigned', 'created') THEN 1 END) / COUNT(*), 1) AS pct_event_type_in_resolved_assig,
        MAX(event_at) AS last_event_at,
        COUNT(*) FILTER (WHERE actor IS NOT NULL) AS n_actor_is_not_null,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list
    FROM prepared_events
    GROUP BY actor, DATE_TRUNC('MONTH', event_at)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(n_actor_is_not_null, 3) OVER (PARTITION BY n_actor_is_not_null ORDER BY n_actor_is_not_null ASC, actor, event_at_month) AS next3_n_actor_is_not_null,
        COUNT(*) OVER (PARTITION BY n_actor_is_not_null) AS rows_in_n_actor_is_not_null,
        ROUND((UNIX_TIMESTAMP(event_at_month) - UNIX_TIMESTAMP(LAG(event_at_month) OVER w_actor_by_pct_event_type_in_r)) / 60, 1) AS minutes_since_prev_event_at_month,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY n_actor_is_not_null)) AS distinct_actor_in_n_actor_is_not_null,
        ROUND(n_actor_is_not_null - AVG(n_actor_is_not_null) OVER w_n_actor_is_not_null, 2) AS n_actor_is_not_null_vs_n_actor_is_not_null_avg,
        SUM(n_actor_is_not_null) OVER (PARTITION BY actor ORDER BY pct_event_type_in_resolved_assig, actor, event_at_month ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS n_actor_is_not_null_sum_2_prior
    FROM stats
    WINDOW w_actor_by_pct_event_type_in_r AS (PARTITION BY actor ORDER BY pct_event_type_in_resolved_assig, actor, event_at_month), w_n_actor_is_not_null AS (PARTITION BY n_actor_is_not_null)
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(rows_in_n_actor_is_not_null) OVER (PARTITION BY distinct_actor_in_n_actor_is_not_null, n_actor_is_not_null ORDER BY last_event_at NULLS LAST, actor, event_at_month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS rows_in_n_actor_is_not_null_sum_1_prior,
        SUM(next3_n_actor_is_not_null) OVER w_actor AS actor_sum_next3_n_actor_is_not_null,
        COUNT(*) OVER w_actor AS rows_in_actor
    FROM ranked
    WINDOW w_actor AS (PARTITION BY actor)
)
SELECT
    actor,
    event_at_month,
    actor_list,
    pct_event_type_in_resolved_assig,
    last_event_at,
    event_type_list,
    next3_n_actor_is_not_null,
    rows_in_n_actor_is_not_null,
    minutes_since_prev_event_at_month,
    distinct_actor_in_n_actor_is_not_null,
    rows_in_n_actor_is_not_null_sum_1_prior,
    actor_sum_next3_n_actor_is_not_null,
    rows_in_actor
FROM ranked_2
WHERE actor = 'customer'
ORDER BY actor_sum_next3_n_actor_is_not_null DESC, actor, event_at_month
LIMIT 10
