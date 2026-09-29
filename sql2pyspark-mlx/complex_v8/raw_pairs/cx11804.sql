WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'G%'
      AND YEAR(release_date) = 2024
),
combined_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre,
        a.artist_name
    FROM valid_tracks s
    JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY is_explicit, artist_is_verified)) AS distinct_artist_id_in_is_explicit_artist_is_veri,
        NTILE(3) OVER (PARTITION BY artist_is_verified ORDER BY bpm DESC, id) AS bpm_tercile,
        MAX(duration_sec) OVER (PARTITION BY artist_id, artist_is_verified) AS artist_id_artist_is_verified_max_duration_sec,
        LEAD(duration_sec) OVER (PARTITION BY artist_genre ORDER BY release_date DESC, id) AS next_duration_sec,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY artist_genre ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        LAST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY artist_is_verified ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY artist_id ORDER BY release_date, id)) AS days_since_prev_release_date,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_is_verified ORDER BY bpm DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_6_prior,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY MONTH(release_date), artist_is_verified ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date DESC NULLS FIRST, id), 3) AS release_date_percent_rank,
        LEAD(bpm) OVER (PARTITION BY artist_id ORDER BY release_date, id) AS next_bpm,
        MIN(duration_sec) OVER (PARTITION BY is_explicit, artist_is_verified) AS is_explicit_artist_is_verified_min_duration_sec,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit, artist_is_verified), 0), 4) AS bpm_share_of_is_explicit_artist_is_verified,
        COUNT(*) OVER (PARTITION BY artist_id, artist_is_verified) AS rows_in_artist_id_artist_is_verified
    FROM combined_details_artist
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(next_duration_sec, bpm) AS greatest_next_duration_sec_bpm,
        LENGTH(TRIM(title)) AS title_len
    FROM windowed
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    is_explicit,
    bpm,
    moods,
    artist_is_verified,
    artist_name,
    distinct_artist_id_in_is_explicit_artist_is_veri,
    bpm_tercile,
    artist_id_artist_is_verified_max_duration_sec,
    next_duration_sec,
    duration_sec_avg_6_prior,
    release_date_percent_rank,
    next_bpm,
    is_explicit_artist_is_verified_min_duration_sec,
    bpm_share_of_is_explicit_artist_is_verified,
    rows_in_artist_id_artist_is_verified,
    greatest_next_duration_sec_bpm,
    title_len,
    CASE WHEN artist_genre IN ('Indie', 'HipHop') THEN 'group_a' WHEN artist_genre = 'Classical' THEN 'group_b' ELSE 'other' END AS artist_genre_group
FROM labeled
WHERE greatest_next_duration_sec_bpm < 366
ORDER BY greatest_next_duration_sec_bpm NULLS LAST
