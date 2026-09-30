WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'N%'
      AND duration_sec < 402
),
joined_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.bpm,
        s.moods,
        s.title_prefix,
        a.debut_year AS artist_debut_year
    FROM valid_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.debut_year BETWEEN 1996 AND 2018
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY artist_id ORDER BY release_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(bpm) OVER (PARTITION BY artist_id) AS artist_id_sum_bpm,
        SUM(bpm) OVER (ORDER BY release_date ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS bpm_sum_3_prior,
        COUNT(*) OVER w_artist_id AS rows_in_artist_id,
        MIN(bpm) OVER (PARTITION BY artist_id) AS artist_id_min_bpm,
        MAX(bpm) OVER w_artist_id AS artist_id_max_bpm
    FROM joined_artist
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.bpm,
        s.title_prefix,
        s.artist_debut_year,
        s.running_count,
        s.artist_id_sum_bpm,
        s.bpm_sum_3_prior,
        s.rows_in_artist_id,
        s.artist_id_min_bpm,
        s.artist_id_max_bpm,
        t.mood
    FROM metrics s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT release_date, mood, artist_id_sum_bpm
    FROM flattened
) src
PIVOT (
    SUM(artist_id_sum_bpm)
    FOR mood IN ('focus', 'party', 'workout', 'sad')
)
