WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        csat,
        tags,
        COALESCE(requester_id, 0) AS requester_id_filled,
        TO_DATE(opened_at) AS opened_at_date,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-19 10:00:00'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(requester_id) OVER (PARTITION BY channel ORDER BY opened_at_date DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_4,
        MAX(requester_id) OVER (PARTITION BY agent_id) AS agent_id_max_requester_id
    FROM valid
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.csat,
        s.tags,
        s.requester_id_filled,
        s.csat_per_requester_id,
        s.requester_id_avg_4,
        t.id AS event_id,
        t.event_at,
        t.event_type,
        t.actor AS event_actor
    FROM with_windows s
    JOIN ticket_events t ON t.ticket_id = s.id
),
unique_rows AS (
    -- Keep the earliest row per channel, event_actor
    SELECT
        id,
        agent_id,
        channel,
        csat,
        tags,
        requester_id_filled,
        csat_per_requester_id,
        requester_id_avg_4,
        event_id,
        event_at,
        event_type,
        event_actor
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel, event_actor ORDER BY csat_per_requester_id, id, event_id) AS recency_rank
        FROM detail_rows
    ) ranked
    WHERE recency_rank = 1
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM unique_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_at >= TIMESTAMP '2025-01-13 11:00:00'
        )
      AND s.csat_per_requester_id >= 3.0
)
-- Final projection
SELECT
    id,
    agent_id,
    channel,
    csat,
    tags,
    requester_id_filled,
    csat_per_requester_id,
    event_id,
    event_at,
    event_type,
    event_actor
FROM qualified
ORDER BY channel DESC, requester_id_filled DESC, id, event_id
LIMIT 10
