WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label
    FROM ticket_events
    WHERE actor <> 'customer'
),
combined_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.requester_id AS ticket_requester_id
    FROM filtered s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM combined_details
    WHERE ticket_requester_id > (SELECT AVG(ticket_requester_id) FROM combined_details)
),
grouped AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_ym,
        event_type,
        ticket_id,
        SUM(CASE WHEN event_type = 'reopened' THEN 1 ELSE 0 END) AS n_event_type_reopened,
        COUNT(*) AS n_rows,
        SUM(ticket_requester_id) AS total_ticket_requester_id
    FROM screened
    WHERE TO_DATE(event_at) < DATE '2025-02-18'
    GROUP BY 1, 2, 3
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.event_at_ym,
        s.event_type,
        s.ticket_id,
        s.n_event_type_reopened,
        s.n_rows,
        s.total_ticket_requester_id,
        t.channel AS ticket_channel,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM grouped s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
unique_rows AS (
    -- Keep the latest row per ticket_channel
    SELECT
        event_at_ym,
        event_type,
        ticket_id,
        n_event_type_reopened,
        n_rows,
        total_ticket_requester_id,
        ticket_channel,
        agent_name,
        agent_hired_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_channel ORDER BY agent_hired_on DESC, event_at_ym DESC, event_type DESC, ticket_id DESC) AS rn
        FROM with_parent
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    event_at_ym,
    n_event_type_reopened,
    total_ticket_requester_id,
    ticket_channel,
    agent_hired_on
FROM unique_rows
ORDER BY agent_hired_on
