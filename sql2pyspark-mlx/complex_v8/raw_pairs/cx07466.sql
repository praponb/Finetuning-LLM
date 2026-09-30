WITH valid AS (
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
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text
    FROM tracks
    WHERE SIZE(moods) >= 1
      AND artist_id >= 2
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        moods_text,
        ROUND(PERCENT_RANK() OVER w_is_explicit_artist_id_by_dur, 3) AS duration_sec_percent_rank,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date) ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date) ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        COUNT(*) OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date) ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MAX(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_max_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date) ORDER BY bpm DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_sum_7
    FROM valid
    WINDOW w_is_explicit_artist_id_by_dur AS (PARTITION BY is_explicit, artist_id ORDER BY duration_sec NULLS LAST, id)
),
per_label AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods_text,
        s.duration_sec_percent_rank,
        s.second_duration_sec,
        s.first_known_duration_sec,
        s.running_count,
        s.artist_id_max_duration_sec,
        s.duration_sec_sum_7,
        t.mood
    FROM sequenced s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        SUBSTRING(TRIM(mood), 1, 3) AS mood_prefix
    FROM per_label
)
-- Subtotals with CUBE
SELECT
    artist_id,
    mood,
    is_explicit,
    ARRAY_SORT(COLLECT_SET(title)) AS title_list
FROM scored
GROUP BY CUBE(artist_id, mood, is_explicit)
ORDER BY artist_id NULLS LAST, mood NULLS LAST, is_explicit NULLS LAST
