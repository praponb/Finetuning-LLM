WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        DAYOFWEEK(release_date) AS release_date_dayofweek
    FROM tracks
    WHERE artist_id IS NOT NULL
),
per_label AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.duration_sec_filled,
        s.release_date_dayofweek,
        t.mood_pos,
        t.mood
    FROM scoped s
    LATERAL VIEW POSEXPLODE(s.moods) t AS mood_pos, mood
),
mood_stats AS (
    -- Max release_date_dayofweek per mood
    SELECT mood, MAX(release_date_dayofweek) AS max_release_date_dayofweek
    FROM per_label
    GROUP BY mood
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_release_date_dayofweek,
        ROUND(s.release_date_dayofweek - g.max_release_date_dayofweek, 2) AS release_date_dayofweek_minus_max
    FROM per_label s
    JOIN mood_stats g ON g.mood = s.mood
    WHERE s.release_date_dayofweek = g.max_release_date_dayofweek
),
unique_rows AS (
    -- Keep the latest row per artist_id, mood
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        duration_sec_filled,
        release_date_dayofweek,
        mood_pos,
        mood,
        max_release_date_dayofweek,
        release_date_dayofweek_minus_max
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id, mood ORDER BY max_release_date_dayofweek DESC, id DESC, mood_pos DESC) AS row_num
        FROM vs_group
    ) ranked
    WHERE row_num = 1
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(release_date_dayofweek / NULLIF(duration_sec_filled, 0), 3) AS release_date_dayofweek_per_duration_sec_filled,
        LOWER(REGEXP_REPLACE(TRIM(mood), ' ', '_')) AS mood_slug,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix
    FROM unique_rows
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count
    FROM labeled s
)
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    bpm,
    duration_sec_filled,
    release_date_dayofweek,
    max_release_date_dayofweek,
    release_date_dayofweek_minus_max,
    release_date_dayofweek_per_duration_sec_filled,
    mood_slug,
    title_prefix,
    track_count,
    LOWER(TRIM(title)) AS title_clean
FROM summarized_rows
ORDER BY title_clean
