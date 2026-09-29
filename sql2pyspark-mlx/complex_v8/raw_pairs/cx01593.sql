WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label,
        LENGTH(TRIM(device)) AS device_len
    FROM plays
    WHERE NOT is_skipped
      AND track_id <= 20
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, MIN(played_at) AS first_played_at
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at
    FROM valid_plays s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        device_len,
        device,
        listener_id,
        MIN(ms_played) AS min_ms_played,
        ROUND(STDDEV(ms_played), 2) AS stddev_ms_played,
        ROUND(SUM(ms_played) / NULLIF(SUM(device_len), 0), 3) AS ms_played_to_device_len_ratio,
        SUM(CASE WHEN is_skipped = TRUE THEN 1 ELSE 0 END) AS n_is_skipped_true,
        COUNT(DISTINCT track_id) AS distinct_track_ids,
        MAX(ms_played) AS max_ms_played
    FROM with_counts
    GROUP BY 1, 2, 3
    HAVING SUM(ms_played) > 56848
),
with_details_listener AS (
    -- Join listeners
    SELECT
        s.device_len,
        s.device,
        s.listener_id,
        s.min_ms_played,
        s.stddev_ms_played,
        s.ms_played_to_device_len_ratio,
        s.n_is_skipped_true,
        s.distinct_track_ids,
        l.interests AS listener_interests,
        l.birth_year AS listener_birth_year
    FROM totals s
    JOIN listeners l ON l.id = s.listener_id
    WHERE SIZE(l.interests) >= 1
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        stddev_ms_played + min_ms_played AS stddev_ms_played_plus_min_ms_played,
        GREATEST(n_is_skipped_true, min_ms_played) AS greatest_n_is_skipped_true_min_ms_played
    FROM with_details_listener
),
qualified AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM labeled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND l.birth_year > 1968
              AND UPPER(TRIM(l.username)) LIKE 'G%'
        )
)
-- Final projection
SELECT
    device_len,
    listener_id,
    min_ms_played,
    n_is_skipped_true,
    distinct_track_ids,
    listener_interests,
    listener_birth_year,
    stddev_ms_played_plus_min_ms_played,
    greatest_n_is_skipped_true_min_ms_played,
    ROUND(stddev_ms_played / NULLIF(min_ms_played, 0), 3) AS stddev_ms_played_per_min_ms_played
FROM qualified
ORDER BY n_is_skipped_true, device_len DESC
