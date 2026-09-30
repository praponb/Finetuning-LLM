WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label
    FROM ticket_events
    WHERE event_type IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        NTILE(5) OVER (ORDER BY event_at ASC, id) AS event_at_quintile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY TO_DATE(event_at), ticket_id ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(PERCENT_RANK() OVER w_event_at_to_date_ticket_id_b_2, 3) AS event_at_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id) AS rn,
        DENSE_RANK() OVER (ORDER BY event_at) AS event_at_dense_rank,
        LAST_VALUE(event_at) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at
    FROM valid_events
    WINDOW w_event_at_to_date_ticket_id_b_2 AS (PARTITION BY TO_DATE(event_at), ticket_id ORDER BY event_at DESC)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_at_quintile,
        minutes_since_prev_event_at,
        rn,
        event_at_dense_rank,
        last_event_at,
        ROUND(rn / NULLIF(event_at_percent_rank, 0), 3) AS rn_per_event_at_percent_rank,
        TO_DATE(event_at) AS event_at_date
    FROM with_trends
)
SELECT
    id,
    ticket_id,
    event_at,
    event_at_quintile,
    event_at_dense_rank,
    last_event_at,
    rn_per_event_at_percent_rank,
    event_at_date,
    ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(last_event_at)) / 3600, 1) AS hours_last_event_at_to_event_at,
    MONTH(last_event_at) AS last_event_at_month
FROM enriched
WHERE event_at_dense_rank BETWEEN 3 AND 10
