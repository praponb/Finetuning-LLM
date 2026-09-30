WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        csat,
        ABS(requester_id - 18) AS requester_id_distance
    FROM tickets
    WHERE MONTH(opened_at) IN (1, 2)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        requester_id_distance,
        COUNT(*) OVER (PARTITION BY priority, csat) AS rows_in_priority_csat,
        RANK() OVER (PARTITION BY agent_id ORDER BY csat DESC, id) AS csat_rank
    FROM prepared
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.rows_in_priority_csat,
        s.csat_rank,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor
    FROM ranked s
    JOIN ticket_events t ON t.ticket_id = s.id AND TO_DATE(t.event_at) < DATE '2025-02-24'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        rows_in_priority_csat,
        csat_rank,
        event_id,
        event_at,
        event_actor,
        MAX(rows_in_priority_csat) OVER w_event_id AS event_id_max_rows_in_priority_csat,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat_rank ORDER BY csat_rank DESC), 3) AS csat_rank_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), csat_rank ORDER BY event_at, id, event_id) AS rn,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), csat_rank ORDER BY event_at, id, event_id) AS event_at_quartile,
        ROUND(csat_rank / NULLIF(SUM(csat_rank) OVER w_event_id, 0), 4) AS csat_rank_share_of_event_id
    FROM line_level_events
    WINDOW w_event_id AS (PARTITION BY event_id)
)
SELECT
    id,
    agent_id,
    csat_rank,
    event_id,
    event_actor,
    event_id_max_rows_in_priority_csat,
    csat_rank_percent_rank,
    rn,
    csat_rank_share_of_event_id
FROM sequenced
ORDER BY id NULLS LAST
