WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        tags,
        COALESCE(csat, 0) AS csat_filled,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat
    FROM tickets
    WHERE requester_id BETWEEN 4 AND 17
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('HOUR', closed_at) AS closed_at_hour,
        CONCAT(channel, '#', CAST(csat_filled AS STRING)) AS channel_ref,
        ROUND(requester_id_per_csat / NULLIF(csat_filled, 0), 3) AS requester_id_per_csat_per_csat_filled
    FROM valid
    WHERE agent_id IS NOT NULL
),
filtered AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM annotated s
    WHERE s.agent_id IN (
            SELECT t.agent_id
            FROM tickets t
            WHERE NOT ARRAY_CONTAINS(t.tags, 'outage')
              AND t.channel IN ('email', 'phone')
        )
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        csat_filled,
        channel,
        TO_DATE(closed_at) AS closed_at_day,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        COUNT(*) AS n_rows
    FROM filtered
    GROUP BY 1, 2, 3
)
SELECT csat_filled, channel, field, metric_value
FROM (
    SELECT csat_filled, channel, distinct_agent_ids, n_rows
    FROM grouped
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR field IN (distinct_agent_ids, n_rows)
) unp
