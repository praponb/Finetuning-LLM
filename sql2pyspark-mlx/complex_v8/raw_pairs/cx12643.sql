WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit
    FROM tracks
    WHERE bpm < 143
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix
    FROM base
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN duration_sec_bucket >= 354 THEN 'HIGH' WHEN duration_sec_bucket >= 156 THEN 'MEDIUM' ELSE 'LOW' END AS duration_sec_bucket_tier,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket_2,
        FLOOR(duration_sec_bucket / 50) * 50 AS duration_sec_bucket_bucket
    FROM enriched
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        is_explicit,
        CASE WHEN duration_sec >= 402 THEN 'A' WHEN duration_sec >= 209 THEN 'B' ELSE 'C' END AS duration_sec_tier,
        COUNT(*) FILTER (WHERE LOWER(title) LIKE '%ol%') AS n_lower_title_like_ol,
        COUNT(*) FILTER (WHERE is_explicit = FALSE) AS n_is_explicit_false,
        COUNT(*) AS row_count
    FROM scored
    GROUP BY artist_id, is_explicit, CASE WHEN duration_sec >= 402 THEN 'A' WHEN duration_sec >= 209 THEN 'B' ELSE 'C' END
    HAVING SUM(duration_sec_bucket_2) > 206
),
exploded_detail_tracks AS (
    -- One row per track
    SELECT
        s.artist_id,
        s.is_explicit,
        s.duration_sec_tier,
        s.row_count,
        t.id AS track_id,
        t.release_date AS track_release_date,
        t.is_explicit AS track_is_explicit,
        t.duration_sec AS track_duration_sec
    FROM per_group s
    JOIN tracks t ON t.artist_id = s.artist_id AND t.duration_sec < 392
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(row_count / 2) * 2 AS row_count_bucket,
        DATE_FORMAT(track_release_date, 'MMM yyyy') AS track_release_date_label,
        COALESCE(track_duration_sec, 0) AS track_duration_sec_filled
    FROM exploded_detail_tracks
    WHERE row_count >= 5
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS last_played_at
    FROM derived s
)
-- Final projection
SELECT
    artist_id,
    is_explicit,
    row_count,
    track_release_date,
    track_is_explicit,
    track_duration_sec,
    row_count_bucket,
    track_release_date_label,
    track_duration_sec_filled,
    CONCAT(track_release_date_label, '#', CAST(track_id AS STRING)) AS track_release_date_label_ref,
    SUBSTRING(TRIM(track_release_date_label), 1, 4) AS track_release_date_label_prefix
FROM with_counts
