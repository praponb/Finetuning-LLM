WITH filtered AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE bpm > 85
),
with_details_artist AS (
    -- Join artists
    SELECT s.*, a.is_verified AS artist_is_verified
    FROM filtered s
    JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        title_len,
        artist_is_verified,
        ROUND(bpm_per_duration_sec - AVG(bpm_per_duration_sec) OVER (PARTITION BY is_explicit, artist_is_verified), 2) AS bpm_per_duration_sec_vs_is_explicit_artist_is_ve,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY is_explicit, artist_is_verified), 2) AS duration_sec_vs_is_explicit_artist_is_verified_a,
        FIRST_VALUE(bpm) OVER (PARTITION BY artist_is_verified, artist_id ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id) AS duration_sec_change,
        NTH_VALUE(title_len, 2) OVER (PARTITION BY artist_id, artist_is_verified ORDER BY title_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_title_len,
        ROUND(title_len / NULLIF(SUM(title_len) OVER (PARTITION BY is_explicit, artist_is_verified), 0), 4) AS title_len_share_of_is_explicit_artist_is_verifie,
        MIN(duration_sec) OVER (PARTITION BY is_explicit, artist_is_verified) AS is_explicit_artist_is_verified_min_duration_sec,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_is_verified) AS rows_in_is_explicit_artist_is_verified,
        LEAD(title_len, 2, 0) OVER (PARTITION BY artist_id, artist_is_verified ORDER BY title_len, id) AS next2_title_len,
        NTH_VALUE(title_len, 2) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_title_len_2,
        ROUND(AVG(bpm_per_duration_sec) OVER w_is_explicit_artist_is_verifi, 2) AS is_explicit_artist_is_verified_avg_bpm_per_durat,
        SUM(bpm_per_duration_sec) OVER (PARTITION BY artist_is_verified, artist_id ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm_per_duration_sec
    FROM with_details_artist
    WINDOW w_is_explicit_artist_is_verifi AS (PARTITION BY is_explicit, artist_is_verified)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm_per_duration_sec_vs_is_explicit_artist_is_ve,
        first_bpm,
        duration_sec_change,
        second_title_len,
        title_len_share_of_is_explicit_artist_is_verifie,
        is_explicit_artist_is_verified_min_duration_sec,
        rows_in_is_explicit_artist_is_verified,
        next2_title_len,
        second_title_len_2,
        running_bpm_per_duration_sec,
        COUNT(*) OVER (PARTITION BY second_title_len_2) AS rows_in_second_title_len_2,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY second_title_len_2), 0), 4) AS duration_sec_share_of_second_title_len_2,
        FIRST_VALUE(title_len) OVER (ORDER BY is_explicit_artist_is_verified_avg_bpm_per_durat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_title_len,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY second_title_len_2)) AS distinct_artist_id_in_second_title_len_2
    FROM with_windows
)
SELECT
    id,
    artist_id,
    duration_sec,
    bpm_per_duration_sec_vs_is_explicit_artist_is_ve,
    first_bpm,
    duration_sec_change,
    second_title_len,
    title_len_share_of_is_explicit_artist_is_verifie,
    rows_in_is_explicit_artist_is_verified,
    second_title_len_2,
    running_bpm_per_duration_sec,
    duration_sec_share_of_second_title_len_2,
    first_title_len,
    distinct_artist_id_in_second_title_len_2,
    GREATEST(duration_sec_change, next2_title_len) AS greatest_duration_sec_change_next2_title_len
FROM ranked
WHERE artist_id IS NOT NULL
ORDER BY duration_sec NULLS LAST
