WITH base AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd HH:00') AS closed_at_label,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 3600, 1) AS hours_closed_at_to_opened_at,
        COALESCE(csat, 0) AS csat_filled
    FROM tickets
    WHERE MONTH(opened_at) IN (1, 2)
      AND channel = 'email'
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at
    FROM base s
),
agent_id_stats AS (
    -- Median csat_filled per agent_id
    SELECT agent_id, PERCENTILE_APPROX(csat_filled, 0.5) AS median_csat_filled
    FROM profiled
    GROUP BY agent_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_csat_filled,
        ROUND(s.csat_filled - g.median_csat_filled, 2) AS csat_filled_minus_median
    FROM profiled s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.csat_filled >= g.median_csat_filled
)
-- Final projection
SELECT
    id,
    requester_id,
    closed_at,
    csat,
    closed_at_label,
    hours_closed_at_to_opened_at,
    first_event_at,
    median_csat_filled,
    csat_filled_minus_median,
    LOWER(TRIM(closed_at_label)) AS closed_at_label_clean,
    SUBSTRING(TRIM(closed_at_label), 1, 2) AS closed_at_label_prefix
FROM above_average
WHERE requester_id < 22
ORDER BY csat DESC NULLS FIRST, id
