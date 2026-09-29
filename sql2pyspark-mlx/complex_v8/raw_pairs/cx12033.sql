WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.ms_played) AS prior_ms_played_12h
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.listener_id, a.played_at
),
with_parent AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        l.interests AS listener_interests
    FROM in_window s
    JOIN listeners l ON l.id = s.listener_id
)
SELECT
    id,
    listener_id,
    played_at,
    listener_interests,
    DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
FROM with_parent
ORDER BY days_since_played_at DESC
