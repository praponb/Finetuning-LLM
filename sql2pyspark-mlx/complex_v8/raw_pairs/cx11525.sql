WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM ticket_events
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.days_since_event_at,
        t.closed_at AS ticket_closed_at,
        t.opened_at AS ticket_opened_at
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        ticket_closed_at,
        ticket_opened_at,
        CASE WHEN days_since_event_at >= 420 THEN 'large' WHEN days_since_event_at >= 140 THEN 'medium' ELSE 'small' END AS days_since_event_at_tier
    FROM with_details
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        ticket_closed_at,
        ticket_opened_at,
        LAG(ticket_closed_at, 2) OVER w_all_by_ticket_closed_at AS prev2_ticket_closed_at,
        NTILE(5) OVER w_all_by_ticket_closed_at AS ticket_closed_at_quintile,
        ROW_NUMBER() OVER w_all_by_ticket_closed_at AS rn,
        LAST_VALUE(ticket_opened_at) OVER (ORDER BY ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_opened_at
    FROM scored
    WINDOW w_all_by_ticket_closed_at AS (ORDER BY ticket_closed_at DESC, id)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        event_at,
        ticket_closed_at,
        ticket_opened_at,
        ticket_closed_at_quintile,
        rn,
        MONTH(ticket_opened_at) AS ticket_opened_at_month,
        TO_DATE(prev2_ticket_closed_at) AS prev2_ticket_closed_at_date
    FROM windowed
),
filtered_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM enriched
    WHERE ticket_closed_at_quintile < (SELECT MAX(ticket_closed_at_quintile) FROM enriched)
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered_2
    WHERE ticket_opened_at_month < (SELECT MAX(ticket_opened_at_month) FROM filtered_2)
)
SELECT
    id,
    event_at,
    ticket_closed_at,
    ticket_closed_at_quintile,
    rn,
    ticket_opened_at_month,
    prev2_ticket_closed_at_date,
    DATE_TRUNC('YEAR', ticket_closed_at) AS ticket_closed_at_year
FROM without_matches
ORDER BY ticket_closed_at_quintile, ticket_opened_at_month DESC
