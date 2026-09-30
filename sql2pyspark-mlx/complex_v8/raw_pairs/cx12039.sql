WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label_2,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE listener_id IS NOT NULL
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS first_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS first_played_at_2
    FROM valid_plays s
),
combined_details_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.is_skipped_label,
        s.ms_played_filled,
        s.first_played_at,
        s.first_played_at_2,
        t.bpm AS track_bpm
    FROM profiled s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.bpm > 76
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS first_played_at_3,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count
    FROM combined_details_track s
),
track_id_stats AS (
    -- Median track_bpm per track_id
    SELECT track_id, PERCENTILE_APPROX(track_bpm, 0.5) AS median_track_bpm
    FROM with_stats
    GROUP BY track_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_track_bpm,
        ROUND(s.track_bpm - g.median_track_bpm, 2) AS track_bpm_minus_median
    FROM with_stats s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.track_bpm >= g.median_track_bpm
),
summary AS (
    -- Summarise per group
    SELECT
        listener_id,
        play_count,
        is_skipped_label,
        SUM(track_bpm_minus_median) AS total_track_bpm_minus_median,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(ms_played_filled) AS total_ms_played_filled,
        COUNT(*) AS row_count
    FROM compared
    GROUP BY listener_id, play_count, is_skipped_label
    HAVING SUM(play_count) > 2
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(is_skipped_label), ' ', '_')) AS is_skipped_label_slug
    FROM summary
    WHERE listener_id <= 11
)
SELECT listener_id, play_count, metric, value
FROM (
    SELECT listener_id, play_count, row_count, distinct_ids, total_ms_played_filled
    FROM tagged
) src
UNPIVOT (
    value FOR metric IN (row_count AS m_row_count, distinct_ids AS m_distinct_ids, total_ms_played_filled AS m_total_ms_played_filled)
) unp
