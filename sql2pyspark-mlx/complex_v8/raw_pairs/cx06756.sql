WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        LEAST(csat, requester_id) AS least_csat_requester_id,
        ABS(csat - 3) AS csat_distance
    FROM tickets
    WHERE SIZE(tags) >= 1
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat_distance,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at,
        t.event_type
    FROM cleaned s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        closed_at,
        event_id,
        event_type,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY channel, agent_id ORDER BY closed_at, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        ROUND(AVG(csat_distance) OVER (PARTITION BY channel, agent_id ORDER BY closed_at, id, event_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS csat_distance_avg_2_prior,
        SIZE(COLLECT_SET(event_type) OVER w_agent_id_event_actor) AS distinct_event_type_in_agent_id_event_actor
    FROM detail_rows
    WINDOW w_agent_id_event_actor AS (PARTITION BY agent_id, event_actor)
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.closed_at,
        s.event_id,
        s.event_type,
        s.second_closed_at,
        s.csat_distance_avg_2_prior,
        s.distinct_event_type_in_agent_id_event_actor,
        t.id AS event_id_2,
        t.event_at,
        t.event_type AS event_event_type
    FROM with_trends s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM exploded_detail s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor = 'system'
              AND t.event_at < TIMESTAMP '2025-02-18 10:00:00'
        )
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM qualified s
    WHERE s.id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.actor NOT IN ('system', 'agent')
              AND t.event_at >= TIMESTAMP '2025-01-25 09:00:00'
        )
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN t.event_type IN ('replied', 'created', 'reopened') THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_event_type_count
    FROM with_activity s
)
-- Final projection
SELECT
    id,
    closed_at,
    event_id,
    event_event_type,
    DAYOFWEEK(closed_at) AS closed_at_dayofweek,
    CONCAT(event_type, '#', CAST(event_event_type_count AS STRING)) AS event_type_ref
FROM summarized_rows
WHERE event_event_type IS NOT NULL
ORDER BY closed_at DESC, id NULLS LAST
