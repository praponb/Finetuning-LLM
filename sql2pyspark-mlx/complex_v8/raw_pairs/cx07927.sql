WITH base_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref,
        CASE WHEN duration_sec >= 344 THEN 'HIGH' WHEN duration_sec >= 213 THEN 'MEDIUM' ELSE 'LOW' END AS duration_sec_tier,
        SIZE(moods) AS moods_count
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND ARRAY_CONTAINS(moods, 'workout')
),
duration_sec_tier_stats AS (
    -- Max moods_count per duration_sec_tier
    SELECT duration_sec_tier, MAX(moods_count) AS max_moods_count
    FROM base_tracks
    GROUP BY duration_sec_tier
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_moods_count,
        ROUND(s.moods_count - g.max_moods_count, 2) AS moods_count_minus_max
    FROM base_tracks s
    JOIN duration_sec_tier_stats g ON g.duration_sec_tier = s.duration_sec_tier
    WHERE s.moods_count = g.max_moods_count
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        SUM(duration_sec) AS total_track_duration_sec,
        COUNT(*) FILTER (WHERE is_explicit = FALSE) AS track_is_explicit_count
    FROM tracks
    GROUP BY artist_id
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.total_track_duration_sec,
        COALESCE(t.track_is_explicit_count, 0) AS track_is_explicit_count
    FROM vs_group s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        UPPER(TRIM(title)) AS title_clean,
        LENGTH(TRIM(title)) AS title_len,
        SIZE(moods) AS moods_count_2
    FROM with_stats
),
stats AS (
    -- Summarise per group
    SELECT
        duration_sec_tier,
        YEAR(release_date) AS release_date_year,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list,
        SUM(CASE WHEN LENGTH(TRIM(title)) >= 7 THEN moods_count_minus_max ELSE 0 END) AS moods_count_minus_max_length_trim_title_7,
        COUNT(DISTINCT duration_sec_tier) AS distinct_duration_sec_tiers
    FROM annotated
    GROUP BY 1, 2
)
SELECT
    duration_sec_tier,
    release_date_year,
    title_list,
    moods_count_minus_max_length_trim_title_7,
    distinct_duration_sec_tiers
FROM stats
