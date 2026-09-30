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
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.prior_ms_played_3d,
        p.id AS play_id,
        p.device AS play_device,
        p.source AS play_source
    FROM matched s
    JOIN plays p ON p.listener_id = s.listener_id AND p.source = 'radio'
)
-- Subtotals with GROUPING SETS
SELECT
    listener_id,
    play_source,
    SUM(prior_ms_played_3d) AS total_prior_ms_played_3d,
    ROUND(STDDEV(prior_ms_played_3d), 2) AS stddev_prior_ms_played_3d
FROM detail_rows_plays
GROUP BY GROUPING SETS ((listener_id, play_source), (listener_id), ())
ORDER BY listener_id NULLS LAST, play_source NULLS LAST
