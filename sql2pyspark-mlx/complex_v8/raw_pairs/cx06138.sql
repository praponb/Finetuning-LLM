WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
enriched AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        t.csat AS ticket_csat,
        t.opened_at AS ticket_opened_at
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        event_type,
        ticket_csat,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM enriched
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(event_type_or_unknown), 1, 2) AS event_type_or_unknown_prefix
    FROM scored
    WHERE ticket_csat >= 2
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(ticket_csat) IGNORE NULLS OVER (PARTITION BY event_type_or_unknown ORDER BY ticket_csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_csat,
        ROW_NUMBER() OVER (PARTITION BY event_type_or_unknown ORDER BY ticket_csat DESC, id) AS ticket_csat_seq,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY event_type_or_unknown ORDER BY ticket_csat DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS ticket_csat_avg_1_prior,
        ROUND(CUME_DIST() OVER w_event_type_or_unknown_by_tic_4, 3) AS ticket_csat_cume_dist,
        ticket_csat - LAG(ticket_csat) OVER (PARTITION BY event_type_or_unknown ORDER BY ticket_csat DESC, id) AS ticket_csat_change
    FROM labeled
    WINDOW w_event_type_or_unknown_by_tic_4 AS (PARTITION BY event_type_or_unknown ORDER BY ticket_csat ASC)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM with_trends
    WHERE ticket_csat_cume_dist > (SELECT MIN(ticket_csat_cume_dist) FROM with_trends)
)
SELECT
    id,
    event_type,
    ticket_csat,
    event_type_or_unknown_prefix,
    first_known_ticket_csat,
    ticket_csat_seq,
    ticket_csat_avg_1_prior,
    ticket_csat_change,
    INITCAP(TRIM(event_type)) AS event_type_clean
FROM screened
ORDER BY first_known_ticket_csat DESC NULLS FIRST, id
LIMIT 10
