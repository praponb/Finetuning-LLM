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
    -- Add calculated columns
    SELECT *, COALESCE(prior_rows_3d, 0) AS prior_rows_3d_filled
    FROM covered
)
SELECT
    id,
    listener_id,
    played_at,
    prior_rows_3d,
    prior_rows_3d_filled,
    GREATEST(prior_ms_played_3d, prior_rows_3d) AS greatest_prior_ms_played_3d_prior_rows_3d,
    CASE WHEN prior_rows_3d >= 6 THEN 'large' WHEN prior_rows_3d >= 3 THEN 'medium' ELSE 'small' END AS prior_rows_3d_tier
FROM enriched
ORDER BY played_at, prior_rows_3d NULLS LAST
