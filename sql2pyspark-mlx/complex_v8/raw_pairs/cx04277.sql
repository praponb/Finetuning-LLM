WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tracks
    WHERE release_date < DATE '2025-04-02'
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        title_ref,
        is_explicit_label,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec
    FROM recent
),
is_explicit_label_stats AS (
    -- Max least_bpm_duration_sec per is_explicit_label
    SELECT is_explicit_label, MAX(least_bpm_duration_sec) AS max_least_bpm_duration_sec
    FROM scored
    GROUP BY is_explicit_label
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_least_bpm_duration_sec,
        ROUND(s.least_bpm_duration_sec - g.max_least_bpm_duration_sec, 2) AS least_bpm_duration_sec_minus_max
    FROM scored s
    JOIN is_explicit_label_stats g ON g.is_explicit_label = s.is_explicit_label
    WHERE s.least_bpm_duration_sec = g.max_least_bpm_duration_sec
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count_2
    FROM outliers s
),
scored_2 AS (
    -- Add calculated columns
    SELECT
        *,
        TO_DATE(last_played_at) AS last_played_at_date,
        ROUND(play_count / NULLIF(duration_sec, 0), 3) AS play_count_per_duration_sec
    FROM summarized_rows
    WHERE max_least_bpm_duration_sec <= 258.0
)
-- Final projection
SELECT
    id,
    duration_sec,
    is_explicit,
    moods,
    title_ref,
    is_explicit_label,
    least_bpm_duration_sec,
    max_least_bpm_duration_sec,
    least_bpm_duration_sec_minus_max,
    play_count,
    last_played_at_date,
    play_count_per_duration_sec
FROM scored_2
