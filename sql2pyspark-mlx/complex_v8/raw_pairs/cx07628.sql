WITH recent AS (
    -- Filter tickets
    SELECT *
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'outage')
      AND opened_at >= TIMESTAMP '2025-01-19 10:00:00'
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug,
        opened_at + INTERVAL 1 DAYS AS opened_at_plus_1d,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM recent
),
screened AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM annotated s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.team = 'Tech'
        )
)
SELECT id, field, metric_value
FROM (
    SELECT id, csat, requester_id
    FROM screened
) src
UNPIVOT (
    metric_value FOR field IN (csat, requester_id)
) unp
