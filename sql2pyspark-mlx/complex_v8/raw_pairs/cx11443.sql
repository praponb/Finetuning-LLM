WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        csat,
        tags,
        DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE SIZE(tags) >= 1
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    csat,
    days_since_opened_at,
    ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text
FROM cleaned_tickets
ORDER BY days_since_opened_at DESC NULLS FIRST, id NULLS LAST
