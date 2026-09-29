WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref
    FROM tracks
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        duration_sec,
        release_date,
        bpm,
        moods,
        is_explicit_label,
        LAST_VALUE(duration_sec) OVER (ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_id), 0), 4) AS duration_sec_share_of_artist_id,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS duration_sec_vs_artist_id_avg
    FROM recent_tracks
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY is_explicit_label) AS is_explicit_label_sum_duration_sec,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_is_explicit_label, 0), 4) AS bpm_share_of_is_explicit_label
    FROM with_trends
    WINDOW w_is_explicit_label AS (PARTITION BY is_explicit_label)
),
enriched AS (
    -- Add calculated columns
    SELECT *, YEAR(release_date) AS release_date_year
    FROM ranked
)
-- Final projection
SELECT
    id,
    duration_sec,
    release_date,
    bpm,
    moods,
    is_explicit_label,
    duration_sec_share_of_artist_id,
    duration_sec_vs_artist_id_avg,
    is_explicit_label_sum_duration_sec,
    bpm_share_of_is_explicit_label,
    release_date_year
FROM enriched
ORDER BY id DESC NULLS FIRST
