WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.requester_id) AS prior_requester_id_3d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
agent_id_stats AS (
    -- Avg prior_rows_3d per agent_id
    SELECT agent_id, AVG(prior_rows_3d) AS avg_prior_rows_3d
    FROM matched
    GROUP BY agent_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_rows_3d,
        ROUND(s.prior_rows_3d - g.avg_prior_rows_3d, 2) AS prior_rows_3d_minus_avg
    FROM matched s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.prior_rows_3d > g.avg_prior_rows_3d
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_rows_3d,
        s.prior_requester_id_3d,
        s.prior_rows_3d_minus_avg,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor,
        t.event_at
    FROM outliers s
    JOIN ticket_events t ON t.ticket_id = s.id
),
joined_event AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_rows_3d_minus_avg,
        s.event_id,
        s.event_type,
        s.event_actor,
        s.event_at,
        t.ticket_id AS event_ticket_id
    FROM line_level s
    JOIN ticket_events t ON t.id = s.event_id
)
-- Final projection
SELECT
    id,
    opened_at,
    prior_rows_3d_minus_avg,
    event_type,
    event_actor,
    event_at,
    event_ticket_id,
    LOWER(REGEXP_REPLACE(TRIM(event_actor), ' ', '_')) AS event_actor_slug,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
FROM joined_event
ORDER BY event_type_or_unknown NULLS LAST
