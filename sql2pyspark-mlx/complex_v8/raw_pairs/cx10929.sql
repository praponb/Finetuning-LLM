WITH joined_ranges AS (
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
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        prior_rows_3d,
        prior_rows_3d - prior_ms_played_3d AS prior_rows_3d_minus_prior_ms_played_3d,
        DATE_TRUNC('HOUR', played_at) AS played_at_hour
    FROM joined_ranges
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        played_at_hour,
        MIN(prior_rows_3d) OVER (PARTITION BY listener_id) AS listener_id_min_prior_rows_3d,
        RANK() OVER (PARTITION BY prior_rows_3d ORDER BY played_at_hour ASC) AS played_at_hour_rank,
        RANK() OVER (PARTITION BY listener_id, prior_rows_3d ORDER BY prior_rows_3d DESC) AS prior_rows_3d_rank
    FROM derived
)
SELECT
    prior_rows_3d_rank,
    played_at_hour_rank,
    PERCENTILE_APPROX(prior_rows_3d_rank, 0.5) AS median_prior_rows_3d_rank,
    SUM(prior_rows_3d_rank) AS total_prior_rows_3d_rank,
    PERCENTILE_APPROX(played_at_hour_rank, 0.5) AS median_played_at_hour_rank
FROM ranked
GROUP BY CUBE(prior_rows_3d_rank, played_at_hour_rank)
ORDER BY prior_rows_3d_rank NULLS LAST, played_at_hour_rank NULLS LAST
