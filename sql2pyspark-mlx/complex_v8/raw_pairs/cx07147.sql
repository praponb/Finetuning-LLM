WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.csat) AS prior_csat_3d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
latest_per_key AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        agent_id,
        opened_at,
        prior_rows_3d,
        prior_csat_3d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY prior_csat_3d DESC, id DESC) AS recency_rank
        FROM covered
    ) ranked
    WHERE recency_rank = 1
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM latest_per_key s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_at >= TIMESTAMP '2025-01-25 13:00:00'
        )
      AND s.prior_csat_3d < 20
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        prior_rows_3d,
        ROUND(prior_csat_3d / NULLIF(prior_rows_3d, 0), 3) AS prior_csat_3d_per_prior_rows_3d,
        ABS(prior_csat_3d - 15) AS prior_csat_3d_distance,
        DATEDIFF(DATE '2025-06-30', opened_at) AS days_since_opened_at
    FROM without_matches
    WHERE prior_csat_3d < 22
)
SELECT
    id,
    agent_id,
    prior_rows_3d,
    prior_csat_3d_per_prior_rows_3d,
    prior_csat_3d_distance,
    days_since_opened_at,
    ROUND(prior_csat_3d_distance / NULLIF(prior_rows_3d, 0), 3) AS prior_csat_3d_distance_per_prior_rows_3d,
    LEAST(days_since_opened_at, prior_csat_3d_distance) AS least_days_since_opened_at_prior_csat_3d_distanc
FROM labeled
WHERE prior_csat_3d_distance < 14
ORDER BY id DESC
