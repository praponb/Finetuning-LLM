WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        LENGTH(TRIM(source)) AS source_len,
        CASE WHEN device IN ('car', 'phone') THEN 'group_a' WHEN device = 'desktop' THEN 'group_b' ELSE 'other' END AS device_group
    FROM plays
    WHERE is_skipped = TRUE
      AND track_id IS NOT NULL
),
unique_rows AS (
    -- Keep the latest row per listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source_len,
        device_group
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY ms_played DESC, id DESC) AS rn
        FROM valid_plays
    ) ranked
    WHERE rn = 1
),
qualified AS (
    -- Keep rows whose track matches
    SELECT s.*
    FROM unique_rows s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.id = s.track_id
              AND t.duration_sec >= 184
        )
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.source_len,
        s.device_group,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped,
        p.source AS play_source
    FROM qualified s
    JOIN plays p ON p.track_id = s.track_id
),
combined_details AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.source_len,
        s.device_group,
        s.play_id,
        s.play_ms_played,
        s.play_device,
        s.play_source,
        t.bpm AS track_bpm,
        l.signup_date AS listener_signup_date
    FROM with_children_plays s
    LEFT JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        track_id,
        device,
        source_len,
        play_id,
        play_device,
        play_source,
        track_bpm,
        listener_signup_date,
        SUM(source_len) OVER (PARTITION BY play_id, play_device) AS play_id_play_device_sum_source_len,
        MAX(play_ms_played) OVER (PARTITION BY play_id, device ORDER BY source_len, id, play_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS play_ms_played_max_6_prior,
        NTH_VALUE(listener_signup_date, 2) OVER (ORDER BY track_bpm DESC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_listener_signup_date,
        COUNT(*) OVER (PARTITION BY play_id, play_device) AS rows_in_play_id_play_device,
        ROUND(track_bpm / NULLIF(SUM(track_bpm) OVER (PARTITION BY play_id, play_device), 0), 4) AS track_bpm_share_of_play_id_play_device
    FROM combined_details
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(listener_signup_date, 'yyyy-MM-dd') AS listener_signup_date_label,
        ABS(track_bpm - 142) AS track_bpm_distance
    FROM ranked
)
SELECT
    id,
    track_id,
    play_id,
    play_device,
    play_source,
    track_bpm,
    listener_signup_date,
    play_id_play_device_sum_source_len,
    play_ms_played_max_6_prior,
    second_listener_signup_date,
    track_bpm_share_of_play_id_play_device,
    listener_signup_date_label,
    track_bpm_distance,
    ROUND(track_bpm_distance / NULLIF(source_len, 0), 3) AS track_bpm_distance_per_source_len,
    INITCAP(TRIM(play_device)) AS play_device_clean
FROM derived
ORDER BY play_device_clean DESC, play_ms_played_max_6_prior
