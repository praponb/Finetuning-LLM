WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.requester_id) AS prior_requester_id_12h
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.agent_id, a.opened_at
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.opened_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_opened_at,
        (SELECT COUNT(*) FILTER (WHERE t.channel <> 'email') FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_channel_count,
        (SELECT SUM(t.requester_id) FROM tickets t WHERE t.agent_id = s.agent_id) AS total_ticket_requester_id
    FROM joined_ranges s
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(first_ticket_opened_at)) / 60, 1) AS minutes_first_ticket_opened_at_to_opened_at
    FROM summarized_rows
    WHERE prior_rows_12h <= 4
)
-- Final projection
SELECT
    id,
    agent_id,
    prior_rows_12h,
    prior_requester_id_12h,
    first_ticket_opened_at,
    ticket_channel_count,
    total_ticket_requester_id,
    minutes_first_ticket_opened_at_to_opened_at,
    ROUND((UNIX_TIMESTAMP(first_ticket_opened_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_first_ticket_opened_at
FROM labeled
WHERE TO_DATE(opened_at) < DATE '2025-02-12'
