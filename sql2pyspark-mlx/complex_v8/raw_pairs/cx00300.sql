WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
    FROM tracks
    WHERE SIZE(moods) >= 2
      AND is_explicit = TRUE
),
with_details AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.moods,
        a.debut_year AS artist_debut_year
    FROM prepared s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods,
        artist_debut_year,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY artist_debut_year), 3) AS artist_debut_year_cume_dist,
        LAG(release_date, 3) OVER (PARTITION BY artist_id ORDER BY artist_debut_year, id) AS prev3_release_date,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec
    FROM with_details
)
SELECT
    id,
    artist_id,
    title,
    release_date,
    bpm,
    moods,
    artist_debut_year,
    prev3_release_date,
    last_known_duration_sec,
    DAYOFWEEK(release_date) AS release_date_dayofweek
FROM with_windows
ORDER BY bpm
