WITH valid AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE (artist_id IS NOT NULL OR release_date BETWEEN DATE '2024-09-26' AND DATE '2025-11-29')
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        release_date_quarter,
        days_since_release_date,
        bpm_per_duration_sec,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref,
        SIZE(moods) AS moods_count
    FROM valid
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY moods_count), 0), 4) AS duration_sec_share_of_moods_count,
        ROUND(days_since_release_date / NULLIF(SUM(days_since_release_date) OVER w_moods_count, 0), 4) AS days_since_release_date_share_of_moods_count,
        LAST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY days_since_release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        ROUND(bpm_per_duration_sec - AVG(bpm_per_duration_sec) OVER (PARTITION BY moods_count), 2) AS bpm_per_duration_sec_vs_moods_count_avg
    FROM scored
    WINDOW w_moods_count AS (PARTITION BY moods_count)
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.days_since_release_date,
        s.bpm_per_duration_sec,
        s.title_ref,
        s.moods_count,
        s.duration_sec_share_of_moods_count,
        s.days_since_release_date_share_of_moods_count,
        s.last_known_release_date,
        p.id AS play_id,
        p.source AS play_source,
        p.played_at AS play_played_at
    FROM with_trends s
    JOIN plays p ON p.track_id = s.id
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    bpm,
    days_since_release_date,
    bpm_per_duration_sec,
    title_ref,
    moods_count,
    duration_sec_share_of_moods_count,
    days_since_release_date_share_of_moods_count,
    last_known_release_date,
    play_played_at,
    CONCAT(title_ref, '#', CAST(play_id AS STRING)) AS title_ref_ref
FROM exploded_detail_plays
ORDER BY title_ref_ref
