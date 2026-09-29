WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('QUARTER', event_at) AS event_at_quarter,
        DATE_TRUNC('DAY', event_at) AS event_at_day
    FROM ticket_events
    WHERE (event_type = 'created' OR actor IN ('customer', 'agent'))
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_actor_ticket_id_by_event_at)) / 60, 1) AS minutes_since_prev_event_at
    FROM valid
    WINDOW w_actor_ticket_id_by_event_at AS (PARTITION BY actor, ticket_id ORDER BY event_at_quarter DESC, id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        actor,
        ROUND(SUM(minutes_since_prev_event_at), 2) AS total_minutes_since_prev_event_at,
        MIN(minutes_since_prev_event_at) AS min_minutes_since_prev_event_at,
        COUNT(*) FILTER (WHERE actor NOT IN ('system', 'agent')) AS n_actor_not_in_system_agent,
        ROUND(AVG(minutes_since_prev_event_at), 2) AS avg_minutes_since_prev_event_at
    FROM sequenced
    WHERE HOUR(event_at) BETWEEN 10 AND 13
    GROUP BY actor
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        INITCAP(TRIM(actor)) AS actor_clean,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix
    FROM per_group
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY n_actor_not_in_system_agent ORDER BY n_actor_not_in_system_agent, actor) AS n_actor_not_in_system_agent_dense_rank,
        SUM(n_actor_not_in_system_agent) OVER (PARTITION BY n_actor_not_in_system_agent) AS n_actor_not_in_system_agent_sum_n_actor_not_in_s,
        FIRST_VALUE(n_actor_not_in_system_agent) OVER (PARTITION BY n_actor_not_in_system_agent ORDER BY n_actor_not_in_system_agent, actor ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_n_actor_not_in_system_agent
    FROM scored
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER w_actor_or_unknown_actor_by_mi AS min_minutes_since_prev_event_at_rank,
        ROUND(n_actor_not_in_system_agent / NULLIF(SUM(n_actor_not_in_system_agent) OVER (PARTITION BY actor_or_unknown, first_n_actor_not_in_system_agent), 0), 4) AS n_actor_not_in_system_agent_share_of_actor_or_un,
        COUNT(*) OVER (PARTITION BY actor_or_unknown, first_n_actor_not_in_system_agent) AS rows_in_actor_or_unknown_first_n_actor_not_in_sy,
        LAST_VALUE(n_actor_not_in_system_agent) OVER (PARTITION BY actor_or_unknown, actor ORDER BY min_minutes_since_prev_event_at DESC, actor ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_n_actor_not_in_system_agent,
        ROUND(CUME_DIST() OVER (PARTITION BY actor_or_unknown, actor ORDER BY min_minutes_since_prev_event_at DESC, actor), 3) AS min_minutes_since_prev_event_at_cume_dist,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY actor_or_unknown, first_n_actor_not_in_system_agent)) AS distinct_actor_in_actor_or_unknown_first_n_actor
    FROM with_windows
    WINDOW w_actor_or_unknown_actor_by_mi AS (PARTITION BY actor_or_unknown, actor ORDER BY min_minutes_since_prev_event_at DESC)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT first_n_actor_not_in_system_agent, last_n_actor_not_in_system_agent, actor_or_unknown, total_minutes_since_prev_event_at
    FROM ranked
) src
PIVOT (
    SUM(total_minutes_since_prev_event_at)
    FOR actor_or_unknown IN ('agent', 'system')
)
ORDER BY first_n_actor_not_in_system_agent, last_n_actor_not_in_system_agent
