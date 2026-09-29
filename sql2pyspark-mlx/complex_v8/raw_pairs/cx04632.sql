WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_TRUNC('WEEK', release_date) AS release_date_week,
        CONCAT(title, '#', CAST(duration_sec AS STRING)) AS title_ref,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE artist_id IS NOT NULL
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.release_date_week,
        s.title_ref,
        s.title_len,
        t.mood
    FROM cleaned s
    LATERAL VIEW OUTER EXPLODE(s.moods) t AS mood
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        title_len,
        mood,
        ABS(title_len - 7) AS title_len_distance,
        SUBSTRING(TRIM(title_ref), 1, 3) AS title_ref_prefix,
        DATE_TRUNC('WEEK', release_date_week) AS release_date_week_week
    FROM flattened
),
deduped AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        title_len,
        mood,
        title_len_distance,
        title_ref_prefix,
        release_date_week_week
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date_week_week, id, mood) AS recency_rank
        FROM enriched
    ) ranked
    WHERE recency_rank = 1
),
screened AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.bpm >= 100
              AND NOT t.is_explicit
        )
      AND s.artist_id <= 2
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        release_date,
        mood,
        title_len_distance,
        title_ref_prefix,
        release_date_week_week,
        ROUND(PERCENT_RANK() OVER (ORDER BY bpm ASC), 3) AS bpm_percent_rank,
        SIZE(COLLECT_SET(artist_id) OVER w_mood) AS distinct_artist_id_in_mood,
        COUNT(*) OVER (PARTITION BY mood) AS rows_in_mood,
        DATEDIFF(release_date, LAG(release_date) OVER w_all_by_bpm) AS days_since_prev_release_date,
        LAG(release_date_week_week) OVER (ORDER BY bpm ASC, id, mood) AS prev_release_date_week_week
    FROM screened
    WINDOW w_mood AS (PARTITION BY mood), w_all_by_bpm AS (ORDER BY bpm ASC, id, mood)
)
-- Final projection
SELECT
    id,
    artist_id,
    release_date,
    mood,
    title_len_distance,
    title_ref_prefix,
    release_date_week_week,
    bpm_percent_rank,
    distinct_artist_id_in_mood,
    days_since_prev_release_date,
    prev_release_date_week_week
FROM windowed
ORDER BY bpm_percent_rank DESC NULLS FIRST, id, mood
LIMIT 10
