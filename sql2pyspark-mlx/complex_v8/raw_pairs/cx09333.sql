WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.ms_played) AS prior_ms_played_1d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
joined_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.prior_ms_played_1d,
        l.username AS listener_username
    FROM covered s
    JOIN listeners l ON l.id = s.listener_id
)
SELECT
    id,
    played_at,
    prior_ms_played_1d,
    listener_username
FROM joined_listener
ORDER BY id DESC, played_at
