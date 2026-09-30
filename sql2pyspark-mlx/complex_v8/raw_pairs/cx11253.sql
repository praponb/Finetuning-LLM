WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix
    FROM ticket_events
    WHERE event_type = 'escalated'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_event_at_year_by_event_at, 3) AS event_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROUND(PERCENT_RANK() OVER w_actor_event_type_by_event_at, 3) AS event_at_percent_rank_2,
        LAG(event_at) OVER (ORDER BY event_at, id) AS prev_event_at,
        DENSE_RANK() OVER w_event_at_year_by_event_at_ti AS event_at_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, event_type ORDER BY event_at), 3) AS event_at_percent_rank_3,
        ROUND(CUME_DIST() OVER (PARTITION BY actor, event_type ORDER BY event_at DESC NULLS FIRST), 3) AS event_at_cume_dist_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(event_at) ORDER BY event_at DESC, id), 3) AS event_at_percent_rank_4,
        ROUND(CUME_DIST() OVER (ORDER BY event_at NULLS LAST, id), 3) AS event_at_cume_dist_3,
        ROUND(PERCENT_RANK() OVER w_all_by_event_at_2, 3) AS event_at_percent_rank_5,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        DENSE_RANK() OVER w_actor_event_type_by_event_at_2 AS event_at_dense_rank_2
    FROM scoped
    WINDOW w_event_at_year_by_event_at AS (PARTITION BY YEAR(event_at) ORDER BY event_at DESC, id), w_actor_event_type_by_event_at AS (PARTITION BY actor, event_type ORDER BY event_at, id), w_event_at_year_by_event_at_ti AS (PARTITION BY YEAR(event_at) ORDER BY event_at DESC), w_all_by_event_at_2 AS (ORDER BY event_at NULLS LAST, id), w_actor_event_type_by_event_at_2 AS (PARTITION BY actor, event_type ORDER BY event_at)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        DATE_TRUNC('YEAR', prev_event_at) AS prev_event_at_year
    FROM windowed
    WHERE actor IS NOT NULL
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    actor_prefix,
    event_at_cume_dist,
    first_event_at,
    event_at_percent_rank_2,
    prev_event_at,
    event_at_dense_rank,
    event_at_percent_rank_3,
    event_at_cume_dist_2,
    event_at_percent_rank_5,
    event_at_dense_rank_2,
    prev_event_at_year,
    MONTH(first_known_event_at) AS first_known_event_at_month,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
FROM scored
ORDER BY first_event_at DESC
