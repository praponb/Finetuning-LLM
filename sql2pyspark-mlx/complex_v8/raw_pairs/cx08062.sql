WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        YEAR(event_at) AS event_at_year
    FROM ticket_events
    WHERE event_type <> 'replied'
      AND actor IN ('customer', 'agent')
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.opened_at AS ticket_opened_at
    FROM base_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.opened_at < TIMESTAMP '2025-02-24 09:00:00'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY actor ORDER BY ticket_opened_at DESC, id), 3) AS ticket_opened_at_cume_dist,
        ROUND(CUME_DIST() OVER w_all_by_event_at_ties, 3) AS event_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type, TO_DATE(ticket_opened_at) ORDER BY ticket_opened_at ASC, id), 3) AS ticket_opened_at_percent_rank,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, actor ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROW_NUMBER() OVER w_event_type_actor_by_event_at_2 AS event_at_seq,
        NTILE(4) OVER (ORDER BY event_at DESC NULLS FIRST, id) AS event_at_quartile,
        ROW_NUMBER() OVER (PARTITION BY actor ORDER BY ticket_opened_at DESC, id) AS ticket_opened_at_seq,
        RANK() OVER (PARTITION BY actor ORDER BY ticket_opened_at DESC, id) AS ticket_opened_at_rank,
        DENSE_RANK() OVER w_actor_by_ticket_opened_at_ti AS ticket_opened_at_dense_rank,
        ROW_NUMBER() OVER w_event_type_ticket_opened_at AS ticket_opened_at_seq_2,
        LAST_VALUE(ticket_opened_at) OVER (ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_opened_at,
        NTILE(10) OVER w_event_type_ticket_opened_at AS ticket_opened_at_decile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor ORDER BY ticket_opened_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type, actor ORDER BY event_at ASC), 3) AS event_at_cume_dist_3,
        RANK() OVER w_all_by_event_at_ties AS event_at_rank
    FROM enriched_ticket
    WINDOW w_all_by_event_at_ties AS (ORDER BY event_at DESC NULLS FIRST), w_event_type_actor_by_event_at_2 AS (PARTITION BY event_type, actor ORDER BY event_at ASC, id), w_actor_by_ticket_opened_at_ti AS (PARTITION BY actor ORDER BY ticket_opened_at DESC), w_event_type_ticket_opened_at AS (PARTITION BY event_type, TO_DATE(ticket_opened_at) ORDER BY ticket_opened_at ASC, id)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(event_at_cume_dist_3, 1, 0) OVER (PARTITION BY event_type, ticket_opened_at_rank ORDER BY last_ticket_opened_at, id) AS prev_event_at_cume_dist_3,
        COUNT(*) OVER (PARTITION BY ticket_opened_at_rank, TO_DATE(event_at)) AS rows_in_ticket_opened_at_rank_event_at_to_date,
        ROUND(CUME_DIST() OVER w_ticket_opened_at_decile_tick, 3) AS ticket_opened_at_seq_2_cume_dist,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, ticket_opened_at_rank ORDER BY last_ticket_opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_2,
        minutes_since_prev_event_at - LAG(minutes_since_prev_event_at) OVER (PARTITION BY ticket_opened_at_decile, ticket_opened_at_rank ORDER BY ticket_opened_at_seq_2 ASC, id) AS minutes_since_prev_event_at_change,
        ROUND(AVG(ticket_opened_at_seq_2) OVER (PARTITION BY ticket_opened_at_decile, ticket_opened_at_rank ORDER BY ticket_opened_at_seq_2 ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ticket_opened_at_seq_2_avg_4
    FROM metrics
    WINDOW w_ticket_opened_at_decile_tick AS (PARTITION BY ticket_opened_at_decile, ticket_opened_at_rank ORDER BY ticket_opened_at_seq_2 ASC)
),
per_group AS (
    -- Summarise per group
    SELECT
        YEAR(ticket_opened_at) AS ticket_opened_at_year,
        event_type,
        MIN(last_ticket_opened_at) AS first_last_ticket_opened_at,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT actor) AS distinct_actors,
        SUM(ticket_opened_at_seq) FILTER (WHERE actor = 'system') AS ticket_opened_at_seq_actor_system,
        ROUND(STDDEV(ticket_opened_at_rank), 2) AS stddev_ticket_opened_at_rank,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids
    FROM ranked
    WHERE ticket_opened_at_seq_2 <= 7
    GROUP BY YEAR(ticket_opened_at), event_type
)
SELECT
    ticket_opened_at_year,
    event_type,
    first_last_ticket_opened_at,
    n_rows,
    distinct_actors,
    ticket_opened_at_seq_actor_system,
    stddev_ticket_opened_at_rank,
    ABS(distinct_actors - 7) AS distinct_actors_distance
FROM per_group
WHERE event_type = 'replied'
