WITH matched AS (
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
stats AS (
    -- Summarise per group
    SELECT
        listener_id,
        prior_rows_3d,
        MIN(played_at) AS first_played_at,
        ROUND(AVG(prior_rows_3d), 2) AS avg_prior_rows_3d,
        ROUND(SUM(prior_ms_played_3d) / NULLIF(SUM(prior_rows_3d), 0), 3) AS prior_ms_played_3d_to_prior_rows_3d_ratio
    FROM matched
    GROUP BY 1, 2
    HAVING MAX(prior_ms_played_3d) > 400000
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE p.device IN ('desktop', 'speaker')) FROM plays p WHERE p.listener_id = s.listener_id) AS play_device_count,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS last_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count
    FROM stats s
)
SELECT
    listener_id,
    prior_rows_3d,
    first_played_at,
    avg_prior_rows_3d,
    play_device_count,
    last_played_at
FROM with_counts
WHERE first_played_at >= TIMESTAMP '2025-01-13 11:00:00'
ORDER BY listener_id, prior_rows_3d
LIMIT 5
