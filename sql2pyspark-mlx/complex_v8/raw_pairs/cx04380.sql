WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.ms_played) AS prior_ms_played_7d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.prior_rows_7d,
        s.prior_ms_played_7d,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped,
        p.source AS play_source
    FROM in_window s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
)
-- Final projection
SELECT
    id,
    listener_id,
    prior_rows_7d,
    prior_ms_played_7d,
    play_id,
    play_device,
    play_is_skipped,
    play_source,
    GREATEST(prior_ms_played_7d, prior_rows_7d) AS greatest_prior_ms_played_7d_prior_rows_7d
FROM exploded_detail
ORDER BY prior_ms_played_7d DESC
