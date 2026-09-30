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
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(prior_rows_3d, prior_ms_played_3d) AS greatest_prior_rows_3d_prior_ms_played_3d,
        CASE WHEN prior_rows_3d >= 6 THEN 'A' WHEN prior_rows_3d >= 2 THEN 'B' ELSE 'C' END AS prior_rows_3d_tier
    FROM matched
    WHERE prior_rows_3d >= 1
)
-- Final projection
SELECT
    id,
    listener_id,
    prior_rows_3d,
    greatest_prior_rows_3d_prior_ms_played_3d,
    prior_rows_3d_tier
FROM enriched
ORDER BY listener_id DESC, greatest_prior_rows_3d_prior_ms_played_3d
