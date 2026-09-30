WITH cleaned AS (
    -- Filter tracks
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
    WHERE title IS NOT NULL
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        t.mood
    FROM cleaned s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
without_matches AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM flattened s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.release_date IS NOT NULL
        )
      AND s.artist_id <= 14
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(mood) OVER w_artist_id) AS distinct_mood_in_artist_id,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS duration_sec_vs_artist_id_avg
    FROM without_matches
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        mood,
        distinct_mood_in_artist_id,
        duration_sec_vs_artist_id_avg,
        FLOOR(bpm / 25) * 25 AS bpm_bucket,
        CASE WHEN mood IN ('chill', 'workout') THEN 'group_a' WHEN mood = 'sad' THEN 'group_b' ELSE 'other' END AS mood_group
    FROM with_trends
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM scored
    WHERE duration_sec > (SELECT MIN(duration_sec) FROM scored)
),
artist_id_stats AS (
    -- Avg bpm_bucket per artist_id
    SELECT artist_id, AVG(bpm_bucket) AS avg_bpm_bucket
    FROM qualified
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_bpm_bucket,
        ROUND(s.bpm_bucket - g.avg_bpm_bucket, 2) AS bpm_bucket_minus_avg
    FROM qualified s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm_bucket > g.avg_bpm_bucket
)
-- Final projection
SELECT DISTINCT mood_group, duration_sec_vs_artist_id_avg
FROM above_average
ORDER BY duration_sec_vs_artist_id_avg DESC, mood_group DESC
