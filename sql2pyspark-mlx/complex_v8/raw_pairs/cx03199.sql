WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        listener_id,
        COUNT(*) AS row_count,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        COUNT(*) FILTER (WHERE is_skipped = TRUE) AS n_is_skipped_true,
        COUNT(DISTINCT source_or_unknown) AS distinct_source_or_unknowns,
        COUNT(*) FILTER (WHERE device IN ('desktop', 'phone', 'tv')) AS n_device_in_desktop_phone_tv
    FROM scoped_plays
    WHERE device IN ('desktop', 'tv')
    GROUP BY listener_id
    HAVING COUNT(*) >= 2
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM totals s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND HOUR(p.played_at) BETWEEN 10 AND 16
              AND p.is_skipped = FALSE
        )
      AND s.listener_id IS NOT NULL
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(*) AS play_count,
        COUNT(*) AS play_count_2,
        COUNT(*) AS play_count_3
    FROM plays
    GROUP BY listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        COALESCE(p.play_count_2, 0) AS play_count_2,
        COALESCE(p.play_count_3, 0) AS play_count_3
    FROM screened s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        listener_id,
        row_count,
        distinct_listener_ids,
        distinct_source_or_unknowns,
        n_device_in_desktop_phone_tv,
        play_count,
        play_count_2,
        COALESCE(play_count_2, 0) AS play_count_2_filled,
        play_count + play_count_3 AS play_count_plus_play_count_3,
        COALESCE(distinct_listener_ids, 0) AS distinct_listener_ids_filled
    FROM summarized_rows
    WHERE play_count_3 > 3
)
-- Final projection
SELECT
    listener_id,
    distinct_listener_ids,
    distinct_source_or_unknowns,
    n_device_in_desktop_phone_tv,
    play_count,
    play_count_2,
    ABS(distinct_listener_ids - 5) AS distinct_listener_ids_distance
FROM tagged
