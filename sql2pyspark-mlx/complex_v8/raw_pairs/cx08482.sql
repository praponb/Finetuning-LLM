WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN source IN ('search', 'playlist') THEN 'group_a' WHEN source = 'radio' THEN 'group_b' ELSE 'other' END AS source_group,
        LENGTH(TRIM(source)) AS source_len,
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix
    FROM plays
    WHERE listener_id <= 13
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count
    FROM filtered_plays s
),
with_parent_track AS (
    -- Attach track attributes
    SELECT
        s.*,
        t.artist_id AS track_artist_id,
        t.release_date AS track_release_date,
        t.moods AS track_moods
    FROM with_counts s
    LEFT JOIN tracks t ON t.id = s.track_id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        listener_id,
        track_id,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        COUNT(*) FILTER (WHERE source IN ('radio', 'search')) AS n_source_in_radio_search,
        COUNT(*) AS n_rows,
        MIN(track_release_date) AS first_track_release_date,
        SUM(CASE WHEN is_skipped = TRUE THEN 1 ELSE 0 END) AS n_is_skipped_true,
        MIN(source_len) AS min_source_len
    FROM with_parent_track
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 3
),
tagged AS (
    -- Label and score rows
    SELECT *, TO_DATE(played_at_month) AS played_at_month_date
    FROM grouped
),
derived AS (
    -- Add calculated columns
    SELECT
        listener_id,
        track_id,
        played_at_month,
        min_source_len,
        FLOOR(n_is_skipped_true / 2) * 2 AS n_is_skipped_true_bucket
    FROM tagged
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM derived
    WHERE min_source_len > (SELECT MIN(min_source_len) FROM derived)
)
-- Final projection
SELECT
    listener_id,
    track_id,
    played_at_month,
    n_is_skipped_true_bucket
FROM filtered
ORDER BY listener_id DESC NULLS FIRST
