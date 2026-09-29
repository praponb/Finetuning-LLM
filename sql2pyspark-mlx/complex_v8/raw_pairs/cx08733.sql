WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label_2
    FROM ticket_events
    WHERE actor <> 'agent'
      AND event_at < TIMESTAMP '2025-02-24 10:00:00'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY actor, ticket_id ORDER BY event_at), 3) AS event_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        LAG(event_at, 2) OVER (ORDER BY event_at ASC, id) AS prev2_event_at,
        LEAD(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id) AS next_event_at,
        RANK() OVER w_all_by_event_at AS event_at_rank,
        ROW_NUMBER() OVER (ORDER BY event_at, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER w_ticket_id_by_event_at_ties, 3) AS event_at_percent_rank
    FROM scoped
    WINDOW w_all_by_event_at AS (ORDER BY event_at ASC, id), w_ticket_id_by_event_at_ties AS (PARTITION BY ticket_id ORDER BY event_at ASC)
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        minutes_since_prev_event_at,
        prev2_event_at,
        next_event_at,
        ROUND((UNIX_TIMESTAMP(prev2_event_at) - UNIX_TIMESTAMP(LAG(prev2_event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', next_event_at), seq_no ORDER BY event_at_rank ASC, id))) / 60, 1) AS minutes_since_prev_prev2_event_at,
        NTILE(5) OVER w_next_event_at_year_event_typ AS next_event_at_quintile,
        ROW_NUMBER() OVER w_next_event_at_year_event_typ AS seq_no_2,
        FIRST_VALUE(event_at_rank) OVER (PARTITION BY YEAR(next_event_at), event_type ORDER BY next_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_rank,
        MIN(event_at_cume_dist) OVER w_event_type AS event_type_min_event_at_cume_dist
    FROM windowed
    WINDOW w_next_event_at_year_event_typ AS (PARTITION BY YEAR(next_event_at), event_type ORDER BY next_event_at DESC, id), w_event_type AS (PARTITION BY event_type)
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    minutes_since_prev_event_at,
    prev2_event_at,
    minutes_since_prev_prev2_event_at,
    next_event_at_quintile,
    seq_no_2,
    event_type_min_event_at_cume_dist,
    ROUND(next_event_at_quintile / NULLIF(seq_no_2, 0), 3) AS next_event_at_quintile_per_seq_no_2
FROM windowed_2
ORDER BY actor DESC NULLS FIRST
