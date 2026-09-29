WITH prepared AS (
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
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm
    FROM tracks
    WHERE artist_id >= 12
),
screened AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM prepared s
    WHERE s.artist_id IN (
            SELECT t.artist_id
            FROM tracks t
            WHERE t.duration_sec BETWEEN 165 AND 402
        )
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, a.debut_year AS artist_debut_year
    FROM screened s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec_per_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        ROUND(AVG(duration_sec_per_bpm) OVER (PARTITION BY artist_id, MONTH(release_date)), 2) AS artist_id_release_date_month_avg_duration_sec_pe
    FROM with_parent
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    bpm,
    moods,
    duration_sec_per_bpm,
    artist_debut_year,
    artist_id_release_date_month_avg_duration_sec_pe,
    LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
    ROUND(duration_sec_per_bpm * 1.5, 2) AS duration_sec_per_bpm_adj
FROM with_windows
ORDER BY id DESC NULLS FIRST, duration_sec_per_bpm_adj NULLS LAST
