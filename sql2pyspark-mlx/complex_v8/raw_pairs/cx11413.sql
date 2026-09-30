WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        CONCAT_WS('-', device, source) AS device_source_label,
        CONCAT(device, '#', CAST(listener_id AS STRING)) AS device_ref
    FROM plays
    WHERE device = 'phone'
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        device,
        is_skipped,
        device_source_label,
        LOWER(REGEXP_REPLACE(TRIM(device_source_label), ' ', '_')) AS device_source_label_slug
    FROM valid
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count,
        (SELECT COUNT(CASE WHEN NOT p.is_skipped THEN 1 END) FROM plays p WHERE p.listener_id = s.listener_id) AS play_is_skipped_count
    FROM annotated s
),
listener_id_stats AS (
    -- Median play_count per listener_id
    SELECT listener_id, PERCENTILE_APPROX(play_count, 0.5) AS median_play_count
    FROM summarized_rows
    GROUP BY listener_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_play_count,
        ROUND(s.play_count - g.median_play_count, 2) AS play_count_minus_median
    FROM summarized_rows s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.play_count >= g.median_play_count
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        COALESCE(play_count_minus_median, 0.0) AS play_count_minus_median_filled
    FROM vs_group
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM scored
    WHERE play_is_skipped_count < (SELECT MAX(play_is_skipped_count) FROM scored)
),
stats AS (
    -- Summarise per group
    SELECT
        is_skipped_label,
        play_count,
        is_skipped,
        MIN(first_played_at) AS first_first_played_at,
        ROUND(AVG(play_count_minus_median), 2) AS avg_play_count_minus_median,
        ROUND(SUM(play_count_minus_median_filled) / NULLIF(SUM(play_count_minus_median), 0), 3) AS play_count_minus_median_filled_to_play_count_min
    FROM filtered
    GROUP BY is_skipped_label, play_count, is_skipped
    HAVING MAX(play_is_skipped_count) > 2
)
-- Final projection
SELECT
    is_skipped_label,
    play_count,
    is_skipped,
    first_first_played_at,
    avg_play_count_minus_median,
    play_count_minus_median_filled_to_play_count_min,
    LENGTH(TRIM(is_skipped_label)) AS is_skipped_label_len
FROM stats
