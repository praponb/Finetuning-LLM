WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE LOWER(title) LIKE '%ig%'
      AND is_explicit = TRUE
),
with_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.moods,
        a.debut_year AS artist_debut_year,
        a.artist_name,
        a.is_verified AS artist_is_verified
    FROM scoped_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE LENGTH(TRIM(a.artist_name)) >= 7
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        title,
        bpm,
        moods,
        artist_name,
        artist_is_verified,
        SUM(bpm) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_id), 0), 4) AS duration_sec_share_of_artist_id,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit ORDER BY bpm), 3) AS bpm_cume_dist,
        SUM(artist_debut_year) OVER (PARTITION BY artist_id) AS artist_id_sum_artist_debut_year,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY bpm DESC) AS bpm_dense_rank,
        NTILE(3) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id) AS bpm_tercile
    FROM with_details_artist
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY artist_id, artist_is_verified) AS rows_in_artist_id_artist_is_verified,
        DENSE_RANK() OVER w_all_by_duration_sec_share_of AS duration_sec_share_of_artist_id_dense_rank,
        ROUND(CUME_DIST() OVER w_bpm_tercile_by_duration_sec, 3) AS duration_sec_share_of_artist_id_cume_dist,
        MAX(bpm_dense_rank) OVER w_bpm_tercile_artist_id AS bpm_tercile_artist_id_max_bpm_dense_rank
    FROM with_trends
    WINDOW w_all_by_duration_sec_share_of AS (ORDER BY duration_sec_share_of_artist_id ASC, id), w_bpm_tercile_by_duration_sec AS (PARTITION BY bpm_tercile ORDER BY duration_sec_share_of_artist_id DESC), w_bpm_tercile_artist_id AS (PARTITION BY bpm_tercile, artist_id)
)
-- Final projection
SELECT
    id,
    title,
    bpm,
    moods,
    artist_name,
    running_bpm,
    duration_sec_share_of_artist_id,
    second_bpm,
    bpm_cume_dist,
    artist_id_sum_artist_debut_year,
    bpm_dense_rank,
    bpm_tercile,
    duration_sec_share_of_artist_id_dense_rank,
    duration_sec_share_of_artist_id_cume_dist,
    bpm_tercile_artist_id_max_bpm_dense_rank,
    FLOOR(running_bpm / 250) * 250 AS running_bpm_bucket,
    CASE WHEN running_bpm >= 1104 THEN 'A' WHEN running_bpm >= 669 THEN 'B' ELSE 'C' END AS running_bpm_tier
FROM with_windows
ORDER BY duration_sec_share_of_artist_id_dense_rank, bpm_tercile DESC
