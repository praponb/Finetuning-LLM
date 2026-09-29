WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref,
        bpm - duration_sec AS bpm_minus_duration_sec,
        TRUNC(release_date, 'MM') AS release_date_month_start
    FROM tracks
    WHERE bpm <= 143
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(title)) AS title_len,
        INITCAP(TRIM(title_ref)) AS title_ref_clean
    FROM prepared_tracks
    WHERE bpm_minus_duration_sec >= 168
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE bpm > (SELECT AVG(bpm) FROM tagged)
),
exploded AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.title_ref,
        s.bpm_minus_duration_sec,
        s.release_date_month_start,
        s.title_len,
        s.title_ref_clean,
        t.mood_pos,
        t.mood
    FROM qualified s
    LATERAL VIEW POSEXPLODE(s.moods) t AS mood_pos, mood
),
mood_stats AS (
    -- Median bpm_minus_duration_sec per mood
    SELECT mood, PERCENTILE_APPROX(bpm_minus_duration_sec, 0.5) AS median_bpm_minus_duration_sec
    FROM exploded
    GROUP BY mood
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_bpm_minus_duration_sec,
        ROUND(s.bpm_minus_duration_sec - g.median_bpm_minus_duration_sec, 2) AS bpm_minus_duration_sec_minus_median
    FROM exploded s
    JOIN mood_stats g ON g.mood = s.mood
    WHERE s.bpm_minus_duration_sec >= g.median_bpm_minus_duration_sec
),
joined AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.bpm_minus_duration_sec,
        s.title_ref_clean,
        s.mood_pos,
        s.median_bpm_minus_duration_sec,
        s.bpm_minus_duration_sec_minus_median,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified
    FROM above_average s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.is_verified = TRUE
),
with_parent AS (
    -- Join artists
    SELECT s.*, a.artist_name
    FROM joined s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT
    id,
    title,
    duration_sec,
    bpm_minus_duration_sec,
    mood_pos,
    median_bpm_minus_duration_sec,
    bpm_minus_duration_sec_minus_median,
    artist_country,
    artist_is_verified,
    INITCAP(TRIM(artist_name)) AS artist_name_clean
FROM with_parent
WHERE UPPER(TRIM(artist_name)) LIKE 'N%'
ORDER BY bpm_minus_duration_sec_minus_median DESC NULLS FIRST, bpm_minus_duration_sec DESC, id, mood_pos
LIMIT 10
