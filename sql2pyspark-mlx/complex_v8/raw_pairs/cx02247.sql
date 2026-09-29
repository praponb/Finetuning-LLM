WITH filtered AS (
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
        CASE WHEN device IN ('car', 'desktop') THEN 'group_a' WHEN device = 'tv' THEN 'group_b' ELSE 'other' END AS device_group,
        MONTH(played_at) AS played_at_month
    FROM plays
    WHERE ms_played < 241585
),
filtered_2 AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.device <> 'car'
              AND HOUR(p.played_at) BETWEEN 8 AND 13
        )
      AND s.device IN ('car', 'tv', 'phone')
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        played_at + INTERVAL 7 DAYS AS played_at_plus_7d,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM filtered_2
),
latest_per_key AS (
    -- Keep the latest row per source_or_unknown
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        device_group,
        played_at_month,
        played_at_plus_7d,
        is_skipped_label,
        source_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source_or_unknown ORDER BY played_at_month DESC, id DESC) AS recency_rank
        FROM annotated
    ) ranked
    WHERE recency_rank = 1
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        device_group,
        played_at_month,
        is_skipped_label,
        source_or_unknown,
        LEAD(played_at_plus_7d, 3) OVER w_device_group_by_ms_played AS next3_played_at_plus_7d,
        COUNT(*) OVER (PARTITION BY is_skipped) AS rows_in_is_skipped,
        RANK() OVER (PARTITION BY track_id ORDER BY played_at_month DESC, id) AS played_at_month_rank,
        ROUND(AVG(ms_played) OVER (PARTITION BY device_group ORDER BY ms_played, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_6_prior
    FROM latest_per_key
    WINDOW w_device_group_by_ms_played AS (PARTITION BY device_group ORDER BY ms_played, id)
),
with_parent_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.played_at_month,
        s.is_skipped_label,
        s.next3_played_at_plus_7d,
        t.moods AS track_moods,
        t.is_explicit AS track_is_explicit,
        t.bpm AS track_bpm,
        l.plan AS listener_plan,
        l.interests AS listener_interests
    FROM windowed s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
)
SELECT *
FROM (
    SELECT track_is_explicit, device, track_bpm
    FROM with_parent_track
) src
PIVOT (
    AVG(track_bpm) AS avg_v, SUM(track_bpm) AS sum_v
    FOR device IN ('speaker', 'phone', 'tv', 'desktop')
)
ORDER BY track_is_explicit
