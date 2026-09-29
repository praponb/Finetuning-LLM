WITH base AS (
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
        CONCAT_WS(': ', priority, channel) AS priority_channel_label,
        INITCAP(TRIM(channel)) AS channel_clean,
        UPPER(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE priority IS NOT NULL
),
tagged AS (
    -- Add calculated columns
    SELECT *, ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat
    FROM base
    WHERE priority_channel_label IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, requester_id, csat
    FROM tagged
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (requester_id AS requester_id_v, csat AS csat_v)
) unp
