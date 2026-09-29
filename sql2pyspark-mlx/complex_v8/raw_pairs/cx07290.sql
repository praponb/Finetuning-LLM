WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        UPPER(TRIM(channel)) AS channel_clean,
        ARRAY_CONTAINS(tags, 'refund') AS has_refund,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id
    FROM tickets
    WHERE requester_id >= 2
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM recent_tickets
    WHERE csat < (SELECT MAX(csat) FROM recent_tickets)
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.opened_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_opened_at,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_closed_at
    FROM filtered s
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        channel_clean,
        has_refund,
        last_ticket_opened_at,
        last_ticket_closed_at,
        CONCAT_WS(': ', priority, channel_clean) AS priority_channel_clean_label
    FROM with_stats
)
SELECT *
FROM (
    SELECT channel, last_ticket_opened_at, priority, requester_id
    FROM scored
) src
PIVOT (
    MIN(requester_id) AS min_v, SUM(requester_id) AS sum_v
    FOR priority IN ('P1', 'P2', 'P4')
)
