WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods
    FROM tracks
),
combined_details_artist AS (
    -- Attach artist attributes
    SELECT s.*, a.is_verified AS artist_is_verified, a.artist_name
    FROM filtered_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.is_verified = FALSE
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY duration_sec), 3) AS duration_sec_cume_dist,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        MIN(bpm) OVER (PARTITION BY artist_is_verified, artist_id) AS artist_is_verified_artist_id_min_bpm,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_is_verified_artist_id, 2) AS duration_sec_vs_artist_is_verified_artist_id_avg,
        ROUND(AVG(bpm) OVER (ORDER BY bpm ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_2,
        LAST_VALUE(release_date) OVER (PARTITION BY MONTH(release_date), artist_is_verified ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date,
        MIN(duration_sec) OVER (PARTITION BY artist_is_verified, artist_id) AS artist_is_verified_artist_id_min_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY MONTH(release_date), artist_is_verified ORDER BY release_date, id) AS seq_no,
        duration_sec - LAG(duration_sec) OVER (ORDER BY duration_sec ASC, id) AS duration_sec_change,
        MAX(duration_sec) OVER (PARTITION BY artist_is_verified) AS artist_is_verified_max_duration_sec,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_artist_is_verified, 0), 4) AS bpm_share_of_artist_is_verified
    FROM combined_details_artist
    WINDOW w_artist_is_verified_artist_id AS (PARTITION BY artist_is_verified, artist_id), w_artist_is_verified AS (PARTITION BY artist_is_verified)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        artist_is_verified,
        artist_name,
        duration_sec_cume_dist,
        first_known_duration_sec,
        artist_is_verified_artist_id_min_bpm,
        duration_sec_vs_artist_is_verified_artist_id_avg,
        bpm_avg_2,
        last_release_date,
        artist_is_verified_artist_id_min_duration_sec,
        duration_sec_change,
        artist_is_verified_max_duration_sec,
        bpm_share_of_artist_is_verified
    FROM sequenced
    WHERE seq_no <= 5
      AND LENGTH(TRIM(artist_name)) >= 4
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(first_known_duration_sec) IGNORE NULLS OVER (ORDER BY artist_is_verified_artist_id_min_bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_duration_sec,
        ROUND(CUME_DIST() OVER w_all_by_artist_is_verified_ar_2, 3) AS artist_is_verified_artist_id_min_bpm_cume_dist,
        RANK() OVER w_artist_id_last_release_date AS artist_is_verified_max_duration_sec_rank,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_id), 0), 4) AS duration_sec_share_of_artist_id,
        FIRST_VALUE(last_release_date) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_release_date,
        LAST_VALUE(bpm) OVER (PARTITION BY artist_id, YEAR(last_release_date) ORDER BY artist_is_verified_max_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY artist_id ORDER BY bpm_share_of_artist_is_verified NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        COUNT(*) OVER w_artist_id AS rows_in_artist_id
    FROM top_per_group
    WINDOW w_all_by_artist_is_verified_ar_2 AS (ORDER BY artist_is_verified_artist_id_min_bpm DESC), w_artist_id_last_release_date AS (PARTITION BY artist_id, YEAR(last_release_date) ORDER BY artist_is_verified_max_duration_sec, id), w_artist_id AS (PARTITION BY artist_id)
)
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    bpm,
    moods,
    artist_name,
    first_known_duration_sec,
    duration_sec_vs_artist_is_verified_artist_id_avg,
    bpm_avg_2,
    last_release_date,
    artist_is_verified_artist_id_min_duration_sec,
    duration_sec_change,
    artist_is_verified_max_duration_sec,
    bpm_share_of_artist_is_verified,
    first_known_first_known_duration_sec,
    artist_is_verified_artist_id_min_bpm_cume_dist,
    artist_is_verified_max_duration_sec_rank,
    duration_sec_share_of_artist_id,
    last_bpm,
    ARRAY_CONTAINS(moods, 'focus') AS has_focus,
    CASE WHEN second_duration_sec >= 357 THEN 'large' WHEN second_duration_sec >= 165 THEN 'medium' ELSE 'small' END AS second_duration_sec_tier
FROM with_windows
