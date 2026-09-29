WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (actor IN ('agent', 'system') OR ticket_id IS NOT NULL)
),
stats AS (
    -- Group-level totals
    SELECT
        event_type,
        actor,
        SUM(CASE WHEN event_type = 'assigned' THEN 1 ELSE 0 END) AS n_event_type_assigned,
        COUNT(*) FILTER (WHERE actor NOT IN ('system', 'customer')) AS n_actor_not_in_system_customer,
        COUNT(*) FILTER (WHERE actor IS NOT NULL) AS n_actor_is_not_null,
        MIN(event_at) AS first_event_at,
        ROUND(100 * COUNT(CASE WHEN actor <> 'agent' THEN 1 END) / COUNT(*), 1) AS pct_actor_agent
    FROM scoped
    WHERE TO_DATE(event_at) < DATE '2025-01-31'
    GROUP BY 1, 2
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label,
        LENGTH(TRIM(actor)) AS actor_len,
        pct_actor_agent + n_actor_is_not_null AS pct_actor_agent_plus_n_actor_is_not_null
    FROM stats
    WHERE first_event_at >= TIMESTAMP '2025-01-25 14:00:00'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(actor_len) OVER (PARTITION BY n_actor_is_not_null, n_event_type_assigned ORDER BY actor_len ASC, event_type, actor ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS actor_len_avg_3,
        COUNT(*) OVER (PARTITION BY n_actor_is_not_null, n_event_type_assigned ORDER BY actor_len ASC, event_type, actor ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTH_VALUE(pct_actor_agent, 2) OVER (PARTITION BY n_actor_is_not_null, n_event_type_assigned ORDER BY actor_len ASC, event_type, actor ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_pct_actor_agent,
        MIN(pct_actor_agent) OVER (PARTITION BY event_type, actor) AS event_type_actor_min_pct_actor_agent
    FROM scored
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM sequenced
    WHERE pct_actor_agent_plus_n_actor_is_not_null < (SELECT MAX(pct_actor_agent_plus_n_actor_is_not_null) FROM sequenced)
)
SELECT
    event_type,
    actor,
    n_event_type_assigned,
    n_actor_is_not_null,
    actor_event_type_label,
    actor_len,
    actor_len_avg_3,
    running_count,
    second_pct_actor_agent,
    event_type_actor_min_pct_actor_agent,
    CONCAT(event_type, '#', CAST(n_event_type_assigned AS STRING)) AS event_type_ref,
    DATE_FORMAT(first_event_at, 'yyyy-MM-dd') AS first_event_at_label
FROM qualified
ORDER BY event_type_actor_min_pct_actor_agent NULLS LAST, actor_len DESC
