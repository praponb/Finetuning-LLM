WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
    WHERE event_type NOT IN ('assigned', 'reopened')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY event_at) AS event_at_dense_rank,
        DENSE_RANK() OVER w_ticket_id_by_event_at AS event_at_dense_rank_2,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST, id) AS event_at_seq,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id, actor ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(PERCENT_RANK() OVER w_all_by_event_at, 3) AS event_at_percent_rank,
        LEAD(event_at) OVER (ORDER BY event_at, id) AS next_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, actor ORDER BY event_at), 3) AS event_at_cume_dist,
        NTILE(5) OVER w_all_by_event_at AS event_at_quintile,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        LAST_VALUE(event_at) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROW_NUMBER() OVER (ORDER BY event_at, id) AS rn,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, actor ORDER BY event_at, id) AS rn_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at), 3) AS event_at_percent_rank_2,
        ROUND(CUME_DIST() OVER w_ticket_id_by_event_at_ties, 3) AS event_at_cume_dist_2
    FROM filtered
    WINDOW w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at, id), w_all_by_event_at AS (ORDER BY event_at, id), w_ticket_id_by_event_at_ties AS (PARTITION BY ticket_id ORDER BY event_at)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    event_at_dense_rank,
    event_at_dense_rank_2,
    event_at_seq,
    first_known_event_at,
    minutes_since_prev_event_at,
    event_at_percent_rank,
    next_event_at,
    event_at_cume_dist,
    event_at_quintile,
    last_known_event_at,
    last_event_at,
    rn,
    rn_2,
    event_at_percent_rank_2,
    event_at_cume_dist_2,
    event_at_dense_rank_2 + event_at_cume_dist_2 AS event_at_dense_rank_2_plus_event_at_cume_dist_2
FROM sequenced
ORDER BY event_at DESC NULLS FIRST, event_at_percent_rank_2 DESC
