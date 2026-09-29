WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND channel NOT IN ('chat', 'email')
),
priority_stats AS (
    -- Max requester_id_per_csat per priority
    SELECT priority, MAX(requester_id_per_csat) AS max_requester_id_per_csat
    FROM valid_tickets
    GROUP BY priority
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_requester_id_per_csat,
        ROUND(s.requester_id_per_csat - g.max_requester_id_per_csat, 2) AS requester_id_per_csat_minus_max
    FROM valid_tickets s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.requester_id_per_csat = g.max_requester_id_per_csat
),
agent_id_stats AS (
    -- Median requester_id_per_csat_minus_max per agent_id
    SELECT agent_id, PERCENTILE_APPROX(requester_id_per_csat_minus_max, 0.5) AS median_requester_id_per_csat_minus_max
    FROM outliers
    GROUP BY agent_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_requester_id_per_csat_minus_max,
        ROUND(s.requester_id_per_csat_minus_max - g.median_requester_id_per_csat_minus_max, 2) AS requester_id_per_csat_minus_max_minus_median
    FROM outliers s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.requester_id_per_csat_minus_max >= g.median_requester_id_per_csat_minus_max
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.csat,
        s.max_requester_id_per_csat,
        s.requester_id_per_csat_minus_max,
        s.median_requester_id_per_csat_minus_max,
        s.requester_id_per_csat_minus_max_minus_median,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type
    FROM above_average s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
latest AS (
    -- Keep the earliest row per priority, agent_id
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        max_requester_id_per_csat,
        requester_id_per_csat_minus_max,
        median_requester_id_per_csat_minus_max,
        requester_id_per_csat_minus_max_minus_median,
        event_id,
        event_actor,
        event_type
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority, agent_id ORDER BY requester_id_per_csat_minus_max_minus_median, id, event_id) AS row_num
        FROM line_level_events
    ) ranked
    WHERE row_num = 1
),
labeled AS (
    -- Label and score rows
    SELECT *, COALESCE(csat, 0) AS csat_filled, COALESCE(csat, 0) AS csat_filled_2
    FROM latest
    WHERE requester_id_per_csat_minus_max BETWEEN -6.0 AND 2.0
)
SELECT *
FROM (
    SELECT csat_filled, priority, requester_id_per_csat_minus_max_minus_median
    FROM labeled
) src
PIVOT (
    SUM(requester_id_per_csat_minus_max_minus_median) AS amount, MAX(requester_id_per_csat_minus_max_minus_median) AS max_v
    FOR priority IN ('P3', 'P1')
)
