WITH valid_tracks AS (
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
    WHERE (LOWER(title) LIKE '%ea%' OR duration_sec <= 353)
),
per_label AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.bpm,
        t.mood_pos,
        t.mood
    FROM valid_tracks s
    LATERAL VIEW POSEXPLODE(s.moods) t AS mood_pos, mood
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        bpm,
        mood_pos,
        mood,
        bpm - LAG(bpm) OVER w_artist_id_by_bpm AS bpm_change,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id, mood_pos ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_1_prior
    FROM per_label
    WINDOW w_artist_id_by_bpm AS (PARTITION BY artist_id ORDER BY bpm DESC, id, mood_pos)
)
SELECT
    artist_id,
    mood_pos,
    ROUND(SUM(mood_pos) / NULLIF(SUM(bpm), 0), 3) AS mood_pos_to_bpm_ratio
FROM with_windows
GROUP BY ROLLUP(artist_id, mood_pos)
