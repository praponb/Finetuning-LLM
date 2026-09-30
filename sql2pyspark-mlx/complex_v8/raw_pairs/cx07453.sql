WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        closed_at + INTERVAL 2 HOURS AS closed_at_plus_2h,
        CASE WHEN requester_id >= 15 THEN 'large' WHEN requester_id >= 7 THEN 'medium' ELSE 'small' END AS requester_id_tier
    FROM tickets
    WHERE SIZE(tags) >= 2
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.csat,
        s.tags,
        s.requester_id_tier,
        t.id AS event_id,
        t.event_type,
        t.event_at
    FROM scoped s
    JOIN ticket_events t ON t.ticket_id = s.id
),
deduped AS (
    -- Keep the earliest row per requester_id_tier
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        tags,
        requester_id_tier,
        event_id,
        event_type,
        event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY requester_id_tier ORDER BY closed_at, id, event_id) AS rn
        FROM detail_rows
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    requester_id_tier,
    event_id,
    event_type,
    event_at,
    LENGTH(TRIM(requester_id_tier)) AS requester_id_tier_len,
    COALESCE(csat, 0) AS csat_filled
FROM deduped
