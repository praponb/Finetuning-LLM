WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
    WHERE event_type IN ('escalated', 'replied')
      AND HOUR(event_at) BETWEEN 9 AND 13
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        event_type,
        actor,
        RANK() OVER (ORDER BY event_at DESC) AS event_at_rank,
        ROW_NUMBER() OVER w_all_by_event_at AS rn,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROW_NUMBER() OVER w_all_by_event_at_2 AS event_at_seq,
        LEAD(event_at, 3) OVER (ORDER BY event_at ASC, id) AS next3_event_at
    FROM cleaned
    WINDOW w_all_by_event_at_ties AS (ORDER BY event_at DESC), w_all_by_event_at AS (ORDER BY event_at DESC NULLS FIRST, id), w_all_by_event_at_2 AS (ORDER BY event_at ASC, id)
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM ranked
    WHERE event_at_rank > (SELECT MIN(event_at_rank) FROM ranked)
)
-- Subtotals with GROUPING SETS
SELECT
    event_at_seq,
    actor,
    SUM(event_at_seq) FILTER (WHERE actor = 'customer') AS event_at_seq_actor_customer,
    SUM(CASE WHEN event_type IN ('assigned', 'escalated') THEN 1 ELSE 0 END) AS n_event_type_in_assigned_escal
FROM qualified
GROUP BY GROUPING SETS ((event_at_seq, actor), (event_at_seq), ())
ORDER BY event_at_seq NULLS LAST, actor NULLS LAST
