WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE artist_id <= 2
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.moods,
        s.bpm_per_duration_sec,
        a.artist_name,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM filtered_tracks s
    JOIN artists a ON a.id = s.artist_id
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.bpm_per_duration_sec,
        s.artist_name,
        s.artist_debut_year,
        s.artist_is_verified,
        t.mood
    FROM enriched_artist s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(duration_sec) OVER (ORDER BY bpm_per_duration_sec ASC, id, mood ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        ROUND(AVG(bpm_per_duration_sec) OVER (ORDER BY bpm_per_duration_sec ASC, id, mood ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS bpm_per_duration_sec_avg_7,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_is_verified, artist_id), 2) AS duration_sec_vs_artist_is_verified_artist_id_avg
    FROM flattened
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM ranked s
    WHERE s.id IN (
            SELECT p.track_id
            FROM plays p
            WHERE MONTH(p.played_at) IN (2, 3)
        )
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        title,
        duration_sec,
        bpm_per_duration_sec,
        artist_name,
        artist_debut_year,
        artist_is_verified,
        mood,
        first_duration_sec,
        bpm_per_duration_sec_avg_7,
        duration_sec_vs_artist_is_verified_artist_id_avg,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id, artist_is_verified), 2) AS duration_sec_vs_artist_id_artist_is_verified_avg,
        LAG(bpm_per_duration_sec_avg_7, 1, 0) OVER (PARTITION BY mood, artist_id ORDER BY first_duration_sec DESC, id, mood) AS prev_bpm_per_duration_sec_avg_7,
        LEAD(first_duration_sec, 1, 0) OVER w_mood_artist_id_by_first_dura AS next_first_duration_sec
    FROM with_activity
    WINDOW w_mood_artist_id_by_first_dura AS (PARTITION BY mood, artist_id ORDER BY first_duration_sec DESC, id, mood)
)
SELECT id, mood, metric_name, metric_value
FROM (
    SELECT id, mood, first_duration_sec, next_first_duration_sec, artist_debut_year
    FROM with_trends
) src
UNPIVOT (
    metric_value FOR metric_name IN (first_duration_sec, next_first_duration_sec, artist_debut_year)
) unp
ORDER BY id, mood, metric_name
