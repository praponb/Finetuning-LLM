WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-18'
      AND event_type <> 'escalated'
),
latest AS (
    -- Keep the latest row per actor, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id DESC) AS recency_rank
        FROM recent_events
    ) ranked
    WHERE recency_rank = 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS next_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        ROW_NUMBER() OVER w_ticket_id_by_event_at AS seq_no
    FROM latest
    WINDOW w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at DESC, id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(event_at_cume_dist) OVER (PARTITION BY DATE_TRUNC('MONTH', next_event_at), seq_no) AS next_event_at_month_seq_no_sum_event_at_cume_dis,
        LEAD(seq_no) OVER w_all_by_seq_no AS next_seq_no,
        ROUND(PERCENT_RANK() OVER (ORDER BY seq_no ASC, id), 3) AS seq_no_percent_rank
    FROM ranked
    WINDOW w_all_by_seq_no AS (ORDER BY seq_no ASC, id)
)
-- Subtotals with CUBE
SELECT
    next_seq_no,
    ticket_id,
    seq_no,
    GROUPING_ID() AS grouping_level,
    COUNT(*) FILTER (WHERE actor <> 'customer') AS n_actor_customer
FROM with_windows
GROUP BY CUBE(next_seq_no, ticket_id, seq_no)
