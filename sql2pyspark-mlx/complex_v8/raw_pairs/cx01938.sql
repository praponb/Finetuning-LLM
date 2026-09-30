WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE event_type <> 'escalated'
      AND actor IN ('system', 'customer')
),
joined AS (
    -- Attach ticket attributes
    SELECT s.*, t.channel AS ticket_channel, t.csat AS ticket_csat
    FROM recent s
    JOIN tickets t ON t.id = s.ticket_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at_date,
        event_type_len,
        ticket_channel,
        ticket_csat,
        MAX(ticket_csat) OVER (PARTITION BY event_type_len ORDER BY event_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ticket_csat_so_far,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY ticket_id), 2) AS ticket_id_avg_ticket_csat,
        LAST_VALUE(event_type_len) IGNORE NULLS OVER (PARTITION BY event_type_len ORDER BY event_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_type_len,
        ROUND(CUME_DIST() OVER w_event_type_by_event_type_len, 3) AS event_type_len_cume_dist
    FROM joined
    WINDOW w_event_type_by_event_type_len AS (PARTITION BY event_type ORDER BY event_type_len, id)
)
-- Subtotals with GROUPING SETS
SELECT
    event_type_len,
    COALESCE(ticket_channel, 'ALL') AS ticket_channel,
    last_known_event_type_len,
    MAX(max_ticket_csat_so_far) AS max_max_ticket_csat_so_far
FROM windowed
GROUP BY GROUPING SETS ((event_type_len, ticket_channel, last_known_event_type_len), (event_type_len), (ticket_channel))
