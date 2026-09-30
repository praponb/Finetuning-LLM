WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-24 13:00:00'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST, id) AS prev_event_at,
        ROW_NUMBER() OVER w_event_type_by_event_at AS event_at_seq,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM scoped_events
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at NULLS LAST, id)
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type = 'assigned'
              AND t.event_at < TIMESTAMP '2025-02-12 13:00:00'
        )
),
unique_rows AS (
    -- Keep the latest row per actor, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        prev_event_at,
        event_at_seq,
        second_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, ticket_id ORDER BY event_at_seq DESC, id DESC) AS recency_rank
        FROM qualified
    ) ranked
    WHERE recency_rank = 1
),
unique_rows_2 AS (
    -- Keep the latest row per actor, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        prev_event_at,
        event_at_seq,
        second_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, ticket_id ORDER BY second_event_at DESC, id DESC) AS rn
        FROM unique_rows
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_all_by_prev_event_at, 3) AS prev_event_at_cume_dist,
        COUNT(*) OVER (PARTITION BY event_at_seq, actor) AS rows_in_event_at_seq_actor,
        ROUND(CUME_DIST() OVER (ORDER BY prev_event_at NULLS LAST), 3) AS prev_event_at_cume_dist_2,
        LAG(second_event_at, 2) OVER w_all_by_prev_event_at AS prev2_second_event_at
    FROM unique_rows_2
    WINDOW w_all_by_prev_event_at AS (ORDER BY prev_event_at NULLS LAST, id)
)
SELECT
    id,
    event_at,
    actor,
    prev_event_at,
    second_event_at,
    prev_event_at_cume_dist,
    rows_in_event_at_seq_actor,
    prev_event_at_cume_dist_2,
    prev2_second_event_at,
    ROUND(prev_event_at_cume_dist * 1.07, 2) AS prev_event_at_cume_dist_adj
FROM with_windows
WHERE event_at_seq BETWEEN 3 AND 10
ORDER BY prev_event_at_cume_dist DESC
