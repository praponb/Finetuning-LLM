WITH in_window AS (
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
derived AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(prior_rows_3d, 0) AS prior_rows_3d_filled,
        played_at + INTERVAL 2 HOURS AS played_at_plus_2h
    FROM in_window
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    prior_rows_3d,
    played_at_plus_2h
FROM derived
ORDER BY listener_id DESC, played_at DESC, id
LIMIT 5
