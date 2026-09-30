WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        tags,
        ABS(requester_id - 18) AS requester_id_distance,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM tickets
    WHERE NOT ARRAY_CONTAINS(tags, 'outage')
),
scored AS (
    -- Label and score rows
    SELECT *, LOWER(TRIM(priority)) AS priority_clean
    FROM prepared
    WHERE agent_id >= 9
),
scored_2 AS (
    -- Derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        requester_id_distance,
        LENGTH(TRIM(priority)) AS priority_len,
        LENGTH(TRIM(priority)) AS priority_len_2,
        SIZE(tags) AS tags_count
    FROM scored
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.requester_id) FROM tickets t WHERE t.agent_id = s.agent_id) AS max_ticket_requester_id,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_closed_at,
        (SELECT SUM(t.csat) FROM tickets t WHERE t.agent_id = s.agent_id) AS total_ticket_csat
    FROM scored_2 s
),
qualified AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.channel <> 'web'
        )
)
-- Final projection
SELECT
    id,
    agent_id,
    priority,
    requester_id_distance,
    priority_len,
    priority_len_2,
    tags_count,
    max_ticket_requester_id,
    total_ticket_csat,
    ABS(tags_count - 1) AS tags_count_distance,
    first_ticket_closed_at + INTERVAL 1 DAYS AS first_ticket_closed_at_plus_1d
FROM qualified
WHERE priority_len_2 BETWEEN 6 AND 13
