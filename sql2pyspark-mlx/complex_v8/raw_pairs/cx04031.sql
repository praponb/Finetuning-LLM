WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.requester_id) AS prior_requester_id_1d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        prior_rows_1d,
        LEAST(prior_requester_id_1d, prior_rows_1d) AS least_prior_requester_id_1d_prior_rows_1d,
        GREATEST(prior_requester_id_1d, prior_rows_1d) AS greatest_prior_requester_id_1d_prior_rows_1d,
        ROUND(prior_rows_1d / NULLIF(prior_requester_id_1d, 0), 3) AS prior_rows_1d_per_prior_requester_id_1d
    FROM covered
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.least_prior_requester_id_1d_prior_rows_1d,
        s.greatest_prior_requester_id_1d_prior_rows_1d,
        s.prior_rows_1d_per_prior_requester_id_1d,
        t.id AS event_id,
        t.event_at,
        t.event_type,
        t.actor AS event_actor
    FROM annotated s
    JOIN ticket_events t ON t.ticket_id = s.id
)
SELECT
    COALESCE(event_type, 'ALL') AS event_type,
    agent_id,
    event_id,
    MIN(opened_at) AS first_opened_at,
    ROUND(SUM(greatest_prior_requester_id_1d_prior_rows_1d) / NULLIF(SUM(least_prior_requester_id_1d_prior_rows_1d), 0), 3) AS greatest_prior_requester_id_1d_prior_rows_1d_to_,
    COUNT(*) AS cnt
FROM exploded_detail_events
GROUP BY GROUPING SETS ((event_type, agent_id, event_id), (event_type), (agent_id), ())
ORDER BY event_type NULLS LAST, agent_id NULLS LAST, event_id NULLS LAST
