WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods
    FROM tracks
    WHERE bpm > 76
      AND LENGTH(TRIM(title)) >= 8
),
combined_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.moods,
        a.is_verified AS artist_is_verified
    FROM prepared s
    JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec NULLS LAST, id), 3) AS duration_sec_percent_rank,
        ROUND(AVG(duration_sec) OVER (PARTITION BY YEAR(release_date) ORDER BY duration_sec, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_2,
        RANK() OVER w_artist_is_verified_is_explic AS release_date_rank,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_is_verified), 0), 4) AS duration_sec_share_of_artist_is_verified,
        NTILE(3) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id) AS duration_sec_tercile,
        COUNT(*) OVER (PARTITION BY artist_is_verified) AS rows_in_artist_is_verified,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_is_verified, is_explicit ORDER BY release_date), 3) AS release_date_percent_rank,
        ROUND(AVG(duration_sec) OVER w_artist_is_verified, 2) AS artist_is_verified_avg_duration_sec,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY duration_sec NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        LAG(release_date) OVER (PARTITION BY artist_is_verified, is_explicit ORDER BY release_date, id) AS prev_release_date,
        COUNT(*) OVER (PARTITION BY artist_is_verified, is_explicit) AS rows_in_artist_is_verified_is_explicit,
        LAG(release_date) OVER w_is_explicit_by_duration_sec AS prev_release_date_2,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_is_verified_is_explic_3, 0), 4) AS duration_sec_share_of_artist_is_verified_is_expl,
        NTILE(5) OVER w_all_by_duration_sec AS duration_sec_quintile
    FROM combined_details_artist
    WINDOW w_artist_is_verified_is_explic AS (PARTITION BY artist_is_verified, is_explicit ORDER BY release_date), w_artist_is_verified AS (PARTITION BY artist_is_verified), w_is_explicit_by_duration_sec AS (PARTITION BY is_explicit ORDER BY duration_sec DESC, id), w_artist_is_verified_is_explic_3 AS (PARTITION BY artist_is_verified, is_explicit), w_all_by_duration_sec AS (ORDER BY duration_sec NULLS LAST, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        artist_is_verified,
        duration_sec_percent_rank,
        duration_sec_avg_2,
        duration_sec_share_of_artist_is_verified,
        duration_sec_tercile,
        rows_in_artist_is_verified,
        release_date_percent_rank,
        artist_is_verified_avg_duration_sec,
        first_known_release_date,
        prev_release_date,
        rows_in_artist_is_verified_is_explicit,
        prev_release_date_2,
        duration_sec_share_of_artist_is_verified_is_expl,
        duration_sec_quintile
    FROM windowed
    WHERE release_date_rank = 1
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    is_explicit,
    artist_is_verified,
    duration_sec_avg_2,
    duration_sec_share_of_artist_is_verified,
    duration_sec_tercile,
    rows_in_artist_is_verified,
    artist_is_verified_avg_duration_sec,
    prev_release_date,
    rows_in_artist_is_verified_is_explicit,
    prev_release_date_2,
    duration_sec_quintile,
    ROUND(duration_sec / NULLIF(duration_sec_avg_2, 0), 3) AS duration_sec_per_duration_sec_avg_2
FROM top_rows
ORDER BY rows_in_artist_is_verified_is_explicit, id
LIMIT 20
