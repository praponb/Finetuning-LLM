WITH cleaned_tickets AS (
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
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        closed_at + INTERVAL 15 MINUTES AS closed_at_plus_15m,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd HH:00') AS closed_at_label
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-02-08 22:00:00'
),
priority_or_unknown_stats AS (
    -- Avg csat per priority_or_unknown
    SELECT priority_or_unknown, AVG(csat) AS avg_csat
    FROM cleaned_tickets
    GROUP BY priority_or_unknown
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_csat, ROUND(s.csat - g.avg_csat, 2) AS csat_minus_avg
    FROM cleaned_tickets s
    JOIN priority_or_unknown_stats g ON g.priority_or_unknown = s.priority_or_unknown
    WHERE s.csat > g.avg_csat
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(requester_id, 0) AS requester_id_filled,
        CASE WHEN requester_id >= 21 THEN 'HIGH' WHEN requester_id >= 8 THEN 'MEDIUM' ELSE 'LOW' END AS requester_id_tier
    FROM outliers
    WHERE agent_id IS NOT NULL
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        priority_or_unknown,
        closed_at_label,
        avg_csat,
        csat_minus_avg,
        requester_id_filled,
        DATE_TRUNC('DAY', closed_at) AS closed_at_day,
        COALESCE(requester_id, 0) AS requester_id_filled_2,
        requester_id_filled - csat AS requester_id_filled_minus_csat
    FROM derived
    WHERE ARRAY_CONTAINS(tags, 'bug')
),
qualified AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM annotated s
    WHERE s.agent_id IN (
            SELECT t.agent_id
            FROM tickets t
            WHERE t.priority IS NOT NULL
              AND t.csat >= 2
        )
),
without_matches AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.channel NOT IN ('phone', 'web')
        )
),
deduped AS (
    -- Keep the earliest row per priority_or_unknown
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        priority_or_unknown,
        closed_at_label,
        avg_csat,
        csat_minus_avg,
        requester_id_filled,
        closed_at_day,
        requester_id_filled_2,
        requester_id_filled_minus_csat
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority_or_unknown ORDER BY requester_id_filled_minus_csat, id) AS recency_rank
        FROM without_matches
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    agent_id,
    opened_at,
    csat,
    tags,
    priority_or_unknown,
    closed_at_label,
    avg_csat,
    requester_id_filled,
    closed_at_day,
    requester_id_filled_2,
    ROUND(requester_id_filled_2 / NULLIF(csat, 0), 3) AS requester_id_filled_2_per_csat,
    CASE WHEN avg_csat >= 3.4 THEN 'HIGH' WHEN avg_csat >= 2.4 THEN 'MEDIUM' ELSE 'LOW' END AS avg_csat_tier
FROM deduped
ORDER BY closed_at_day DESC
