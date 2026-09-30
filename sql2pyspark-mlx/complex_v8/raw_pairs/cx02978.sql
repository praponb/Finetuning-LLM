WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE actor = 'system'
      AND ticket_id <= 59
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        actor,
        MAX(event_at) AS last_event_at,
        SUM(CASE WHEN event_type IS NOT NULL THEN 1 ELSE 0 END) AS n_event_type_is_not_null,
        COUNT(*) FILTER (WHERE actor IS NOT NULL) AS n_actor_is_not_null,
        COUNT(*) FILTER (WHERE event_type IS NOT NULL) AS n_event_type_is_not_null_2,
        ROUND(100 * COUNT(CASE WHEN actor = 'agent' THEN 1 END) / COUNT(*), 1) AS pct_actor_agent,
        SUM(CASE WHEN actor IN ('agent', 'system') THEN 1 ELSE 0 END) AS n_actor_in_agent_system
    FROM base
    WHERE event_type = 'replied'
    GROUP BY actor
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        actor,
        last_event_at,
        n_event_type_is_not_null,
        n_actor_is_not_null,
        n_event_type_is_not_null_2,
        pct_actor_agent,
        n_actor_in_agent_system,
        n_event_type_is_not_null - LAG(n_event_type_is_not_null) OVER w_n_event_type_is_not_null_by AS n_event_type_is_not_null_change,
        SUM(n_actor_in_agent_system) OVER (PARTITION BY n_event_type_is_not_null_2, YEAR(last_event_at)) AS n_event_type_is_not_null_2_last_event_at_year_su,
        LAST_VALUE(n_event_type_is_not_null_2) OVER (PARTITION BY n_actor_in_agent_system ORDER BY n_event_type_is_not_null_2 NULLS LAST, actor ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_n_event_type_is_not_null_2,
        MIN(pct_actor_agent) OVER (PARTITION BY n_event_type_is_not_null_2, YEAR(last_event_at)) AS n_event_type_is_not_null_2_last_event_at_year_mi,
        DENSE_RANK() OVER (PARTITION BY n_actor_in_agent_system ORDER BY n_event_type_is_not_null_2 NULLS LAST, actor) AS n_event_type_is_not_null_2_dense_rank,
        LAST_VALUE(pct_actor_agent) OVER (PARTITION BY n_event_type_is_not_null ORDER BY n_event_type_is_not_null_2 DESC NULLS FIRST, actor ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_pct_actor_agent
    FROM summary
    WINDOW w_n_event_type_is_not_null_by AS (PARTITION BY n_event_type_is_not_null ORDER BY n_event_type_is_not_null_2 DESC NULLS FIRST, actor)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(last_pct_actor_agent / 25) * 25 AS last_pct_actor_agent_bucket,
        ROUND(pct_actor_agent * 1.07, 2) AS pct_actor_agent_adj,
        YEAR(last_event_at) AS last_event_at_year
    FROM with_trends
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM scored
    WHERE n_event_type_is_not_null_2_dense_rank = 1
)
SELECT *
FROM (
    SELECT n_actor_is_not_null, actor, pct_actor_agent
    FROM top_rows
) src
PIVOT (
    AVG(pct_actor_agent)
    FOR actor IN ('system', 'customer')
)
ORDER BY n_actor_is_not_null
