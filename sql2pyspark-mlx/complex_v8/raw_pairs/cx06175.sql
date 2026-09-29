WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec,
        ARRAY_CONTAINS(moods, 'workout') AS has_workout
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND duration_sec > 144
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(bpm_per_duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS bpm_per_duration_sec_avg_2_prior,
        ROUND(bpm - AVG(bpm) OVER w_release_date_month, 2) AS bpm_vs_release_date_month_avg,
        ROW_NUMBER() OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC NULLS FIRST, id) AS duration_sec_seq,
        ROUND(AVG(bpm_per_duration_sec) OVER (PARTITION BY MONTH(release_date)), 2) AS release_date_month_avg_bpm_per_duration_sec
    FROM recent
    WINDOW w_release_date_month AS (PARTITION BY MONTH(release_date))
),
filtered AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND NOT a.is_verified
        )
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY has_workout), 0), 4) AS duration_sec_share_of_has_workout,
        SUM(duration_sec_seq) OVER (PARTITION BY is_explicit ORDER BY release_date_month_avg_bpm_per_duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_seq_sum_7
    FROM filtered
),
filtered_2 AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND UPPER(TRIM(t.title)) LIKE 'S%'
        )
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM filtered_2
    WHERE duration_sec > (SELECT MIN(duration_sec) FROM filtered_2)
),
combined_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.bpm_per_duration_sec,
        s.bpm_per_duration_sec_avg_2_prior,
        s.bpm_vs_release_date_month_avg,
        s.duration_sec_seq,
        s.release_date_month_avg_bpm_per_duration_sec,
        s.duration_sec_share_of_has_workout,
        a.genre AS artist_genre
    FROM with_activity s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.is_verified = TRUE
)
SELECT
    is_explicit,
    duration_sec_seq,
    COALESCE(artist_genre, 'ALL') AS artist_genre,
    SUM(bpm) AS total_bpm,
    COUNT(*) AS n_rows,
    COUNT(*) FILTER (WHERE artist_genre IN ('Indie', 'Classical', 'Jazz')) AS n_artist_genre_in_indie_classi
FROM combined_details
GROUP BY ROLLUP(is_explicit, duration_sec_seq, artist_genre)
ORDER BY is_explicit NULLS LAST, duration_sec_seq NULLS LAST, artist_genre NULLS LAST
