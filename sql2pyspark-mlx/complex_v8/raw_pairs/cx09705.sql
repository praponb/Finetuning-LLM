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
annotated AS (
    -- Derive helper columns
    SELECT *, COALESCE(prior_rows_3d, 0) AS prior_rows_3d_filled
    FROM in_window
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(prior_ms_played_3d / NULLIF(SUM(prior_ms_played_3d) OVER (PARTITION BY listener_id, prior_rows_3d), 0), 4) AS prior_ms_played_3d_share_of_listener_id_prior_ro,
        COUNT(*) OVER (PARTITION BY listener_id, prior_rows_3d) AS rows_in_listener_id_prior_rows_3d
    FROM annotated
)
SELECT id, metric_name, reading
FROM (
    SELECT id, prior_rows_3d, prior_ms_played_3d
    FROM metrics
) src
UNPIVOT (
    reading FOR metric_name IN (prior_rows_3d, prior_ms_played_3d)
) unp
ORDER BY id, metric_name
