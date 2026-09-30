WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        tags,
        LOWER(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE (priority IN ('P4', 'P1') OR opened_at >= TIMESTAMP '2025-01-13 12:00:00')
),
derived AS (
    -- Add calculated columns
    SELECT *, DATEDIFF(DATE '2025-12-31', closed_at) AS days_since_closed_at
    FROM prepared_tickets
    WHERE NOT ARRAY_CONTAINS(tags, 'outage')
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        closed_at,
        channel,
        tags,
        priority_clean,
        days_since_closed_at,
        LOWER(TRIM(channel)) AS channel_clean,
        LOWER(REGEXP_REPLACE(TRIM(priority_clean), ' ', '_')) AS priority_clean_slug,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM derived
)
-- Final projection
SELECT
    id,
    requester_id,
    channel,
    channel_clean,
    requester_id_bucket,
    COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
FROM scored
