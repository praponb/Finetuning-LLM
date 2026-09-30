WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type = 'created'
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        t.csat AS ticket_csat,
        t.tags AS ticket_tags
    FROM filtered_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND MONTH(t.opened_at) IN (1, 3)
),
combined_details AS (
    -- Join tickets
    SELECT s.*, t.agent_id AS ticket_agent_id
    FROM with_parent_ticket s
    JOIN tickets t ON t.id = s.ticket_id AND t.requester_id BETWEEN 5 AND 18
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY event_type, ticket_id) AS rows_in_event_type_ticket_id,
        MAX(ticket_csat) OVER (ORDER BY event_at ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS ticket_csat_max_1_prior
    FROM combined_details
),
windowed_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        ticket_csat,
        ticket_tags,
        rows_in_event_type_ticket_id,
        RANK() OVER (PARTITION BY event_type ORDER BY event_at) AS event_at_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_agent_id, event_type ORDER BY ticket_csat_max_1_prior DESC), 3) AS ticket_csat_max_1_prior_percent_rank,
        NTH_VALUE(ticket_csat_max_1_prior, 2) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_csat_max_1_prior,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ticket_csat_avg_7,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER (PARTITION BY ticket_id, ticket_csat), 0), 4) AS ticket_csat_share_of_ticket_id_ticket_csat
    FROM windowed
)
-- Final projection
SELECT
    id,
    event_at,
    rows_in_event_type_ticket_id,
    event_at_rank,
    ticket_csat_max_1_prior_percent_rank,
    second_ticket_csat_max_1_prior,
    ticket_csat_avg_7,
    TO_DATE(event_at) AS event_at_date,
    DATE_TRUNC('MONTH', event_at) AS event_at_month
FROM windowed_2
WHERE ticket_csat_avg_7 < 3.8
ORDER BY second_ticket_csat_max_1_prior NULLS LAST
