WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.tags AS ticket_tags,
        su.agent_name
    FROM recent s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at NULLS LAST, id))) / 60, 1) AS minutes_since_prev_event_at,
        LEAD(event_at, 2) OVER (PARTITION BY actor, event_type ORDER BY event_at NULLS LAST, id) AS next2_event_at,
        LEAD(event_at, 2) OVER (PARTITION BY actor, event_type ORDER BY event_at DESC, id) AS next2_event_at_2,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor, event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        DENSE_RANK() OVER w_actor_event_type_by_event_at_2 AS event_at_dense_rank,
        DENSE_RANK() OVER (PARTITION BY actor, event_type ORDER BY event_at DESC) AS event_at_dense_rank_2,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, event_type ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        LAST_VALUE(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at
    FROM enriched_ticket
    WINDOW w_actor_event_type_by_event_at_2 AS (PARTITION BY actor, event_type ORDER BY event_at DESC, id)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE event_at_dense_rank = 1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        agent_name,
        minutes_since_prev_event_at,
        next2_event_at,
        next2_event_at_2,
        event_at_dense_rank_2,
        first_event_at,
        last_event_at,
        ROW_NUMBER() OVER (PARTITION BY event_at_dense_rank_2 ORDER BY minutes_since_prev_event_at, id) AS rn,
        SUM(event_at_dense_rank) OVER w_next2_event_at_2_to_date_eve AS next2_event_at_2_to_date_event_at_dense_rank_sum,
        NTILE(10) OVER (ORDER BY event_at_dense_rank DESC, id) AS event_at_dense_rank_decile,
        LAST_VALUE(first_event_at) IGNORE NULLS OVER (PARTITION BY event_at_dense_rank_2 ORDER BY minutes_since_prev_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_event_at
    FROM leaders
    WINDOW w_next2_event_at_2_to_date_eve AS (PARTITION BY TO_DATE(next2_event_at_2), event_at_dense_rank)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        event_at_dense_rank_2,
        SUM(rn) AS total_rn,
        MAX(next2_event_at_2) AS last_next2_event_at_2,
        ROUND(SUM(event_at_dense_rank_decile) / NULLIF(SUM(event_at_dense_rank_2), 0), 3) AS event_at_dense_rank_decile_to_event_at_dense_ran,
        MIN(minutes_since_prev_event_at) AS min_minutes_since_prev_event_at,
        COUNT(DISTINCT actor) AS distinct_actors
    FROM sequenced
    GROUP BY 1
    HAVING AVG(event_at_dense_rank_2) > 4
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(event_at_dense_rank_2 - AVG(event_at_dense_rank_2) OVER w_event_at_dense_rank_2_distin, 2) AS event_at_dense_rank_2_vs_event_at_dense_rank_2_d,
        COUNT(*) OVER (ORDER BY min_minutes_since_prev_event_at DESC, event_at_dense_rank_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM totals
    WINDOW w_event_at_dense_rank_2_distin AS (PARTITION BY event_at_dense_rank_2, distinct_actors)
)
-- Final projection
SELECT
    event_at_dense_rank_2,
    total_rn,
    last_next2_event_at_2,
    event_at_dense_rank_decile_to_event_at_dense_ran,
    min_minutes_since_prev_event_at,
    distinct_actors,
    running_count,
    FLOOR(running_count / 5) * 5 AS running_count_bucket
FROM with_windows
ORDER BY event_at_dense_rank_decile_to_event_at_dense_ran
