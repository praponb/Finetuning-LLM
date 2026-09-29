WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        DAYOFWEEK(closed_at) AS closed_at_dayofweek
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 7 AND 11
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.csat) FROM tickets t WHERE t.agent_id = s.agent_id) AS min_ticket_csat,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_closed_at
    FROM cleaned s
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        min_ticket_csat,
        last_ticket_closed_at,
        DATE_FORMAT(last_ticket_closed_at, 'yyyy-MM-dd HH:00') AS last_ticket_closed_at_label,
        LEAST(closed_at_dayofweek, min_ticket_csat) AS least_closed_at_dayofweek_min_ticket_csat,
        CASE WHEN ticket_count >= 6 THEN 'HIGH' WHEN ticket_count >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS ticket_count_tier
    FROM summarized_rows
),
with_activity AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.csat < 3
        )
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM with_activity
    WHERE csat < (SELECT MAX(csat) FROM with_activity)
),
with_activity_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered
    WHERE requester_id < (SELECT MAX(requester_id) FROM filtered)
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    csat,
    min_ticket_csat,
    last_ticket_closed_at_label,
    least_closed_at_dayofweek_min_ticket_csat,
    ticket_count_tier
FROM with_activity_2
ORDER BY min_ticket_csat
