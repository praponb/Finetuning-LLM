WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.ms_played) AS prior_ms_played_3d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
enriched AS (
    -- Attach listener attributes
    SELECT s.*, l.username AS listener_username, l.plan AS listener_plan
    FROM covered s
    JOIN listeners l ON l.id = s.listener_id
)
SELECT
    id,
    played_at,
    prior_rows_3d,
    listener_plan
FROM enriched
WHERE played_at < TIMESTAMP '2025-02-12 10:00:00'
