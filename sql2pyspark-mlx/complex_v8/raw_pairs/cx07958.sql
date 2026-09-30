WITH base_tracks AS (
    -- Scope the tracks rows
    SELECT *
    FROM tracks
    WHERE artist_id IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_min_duration_sec,
        ROUND(AVG(duration_sec) OVER w_artist_id, 2) AS artist_id_avg_duration_sec,
        NTILE(4) OVER w_is_explicit_artist_id_by_bpm AS bpm_quartile,
        LAST_VALUE(release_date) OVER (PARTITION BY is_explicit ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date
    FROM base_tracks
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_is_explicit_artist_id_by_bpm AS (PARTITION BY is_explicit, artist_id ORDER BY bpm ASC, id)
)
-- Subtotals with GROUPING SETS
SELECT
    artist_id,
    bpm_quartile,
    GROUPING_ID() AS gid,
    ROUND(SUM(artist_id_avg_duration_sec) / NULLIF(SUM(bpm), 0), 3) AS artist_id_avg_duration_sec_to_bpm_ratio
FROM ranked
GROUP BY GROUPING SETS ((artist_id, bpm_quartile), (artist_id), ())
