WITH prepared AS (
    -- Scope the tracks rows
    SELECT *
    FROM tracks
    WHERE release_date >= DATE '2024-11-14'
      AND NOT is_explicit
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.moods,
        a.genre AS artist_genre
    FROM prepared s
    JOIN artists a ON a.id = s.artist_id
),
totals AS (
    -- Group-level totals
    SELECT
        artist_genre,
        artist_id,
        is_explicit,
        MAX(duration_sec) AS max_duration_sec,
        MIN(duration_sec) AS min_duration_sec,
        ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
        COUNT(*) FILTER (WHERE is_explicit = TRUE) AS n_is_explicit_true
    FROM combined_details_artist
    GROUP BY 1, 2, 3
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(3) OVER (ORDER BY min_duration_sec DESC, artist_genre, artist_id, is_explicit) AS min_duration_sec_tercile,
        MAX(avg_duration_sec) OVER (ORDER BY min_duration_sec DESC, artist_genre, artist_id, is_explicit ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_duration_sec_max_2,
        MAX(avg_duration_sec) OVER (PARTITION BY n_is_explicit_true, artist_id) AS n_is_explicit_true_artist_id_max_avg_duration_se,
        COUNT(*) OVER w_n_is_explicit_true_artist_id AS rows_in_n_is_explicit_true_artist_id
    FROM totals
    WINDOW w_n_is_explicit_true_artist_id AS (PARTITION BY n_is_explicit_true, artist_id)
)
SELECT
    artist_genre,
    artist_id,
    max_duration_sec,
    min_duration_sec,
    avg_duration_sec,
    n_is_explicit_true,
    min_duration_sec_tercile,
    avg_duration_sec_max_2,
    n_is_explicit_true_artist_id_max_avg_duration_se,
    rows_in_n_is_explicit_true_artist_id
FROM with_windows
WHERE NOT is_explicit
ORDER BY artist_genre, max_duration_sec DESC
