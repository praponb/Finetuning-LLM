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
filtered AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM covered s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND l.signup_date >= DATE '2024-08-02'
              AND ARRAY_CONTAINS(l.interests, 'kpop')
        )
)
SELECT
    id,
    listener_id,
    played_at,
    prior_rows_3d,
    COALESCE(prior_rows_3d, 0) AS prior_rows_3d_filled
FROM filtered
ORDER BY id
