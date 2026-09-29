WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        moods,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec,
        UPPER(TRIM(title)) AS title_clean
    FROM tracks
),
artist_id_stats AS (
    -- Avg bpm_per_duration_sec per artist_id
    SELECT artist_id, AVG(bpm_per_duration_sec) AS avg_bpm_per_duration_sec
    FROM cleaned
    GROUP BY artist_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_bpm_per_duration_sec,
        ROUND(s.bpm_per_duration_sec - g.avg_bpm_per_duration_sec, 2) AS bpm_per_duration_sec_minus_avg
    FROM cleaned s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm_per_duration_sec > g.avg_bpm_per_duration_sec
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        CASE WHEN bpm_per_duration_sec >= 7.5 THEN 'A' WHEN bpm_per_duration_sec >= 3.0 THEN 'B' ELSE 'C' END AS bpm_per_duration_sec_tier
    FROM vs_group
)
SELECT
    id,
    duration_sec,
    is_explicit,
    moods,
    avg_bpm_per_duration_sec,
    bpm_per_duration_sec_minus_avg,
    is_explicit_label,
    bpm_per_duration_sec_tier,
    FLOOR(avg_bpm_per_duration_sec / 2) * 2 AS avg_bpm_per_duration_sec_bucket
FROM enriched
ORDER BY id DESC NULLS FIRST
