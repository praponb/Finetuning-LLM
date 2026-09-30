WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        ABS(ms_played - 183484) AS ms_played_distance
    FROM plays
    WHERE track_id IS NOT NULL
      AND is_skipped = TRUE
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        played_at,
        device,
        device_slug,
        is_skipped_label,
        ms_played_distance,
        CONCAT(is_skipped_label, '#', CAST(track_id AS STRING)) AS is_skipped_label_ref,
        LENGTH(TRIM(is_skipped_label)) AS is_skipped_label_len
    FROM base_plays
),
unique_rows AS (
    -- Keep the latest row per listener_id
    SELECT
        id,
        listener_id,
        played_at,
        device,
        device_slug,
        is_skipped_label,
        ms_played_distance,
        is_skipped_label_ref,
        is_skipped_label_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY ms_played_distance DESC, id DESC) AS row_num
        FROM labeled
    ) ranked
    WHERE row_num = 1
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.device,
        s.is_skipped_label_ref,
        s.is_skipped_label_len,
        p.id AS play_id,
        p.source AS play_source,
        p.track_id AS play_track_id
    FROM unique_rows s
    JOIN plays p ON p.listener_id = s.listener_id AND p.track_id IS NOT NULL
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.play_track_id) AS first_played_at,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.track_id = s.play_track_id) AS total_play_ms_played,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.track_id = s.play_track_id) AS avg_play_ms_played
    FROM exploded_detail_plays s
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.play_id,
        s.play_source,
        s.play_track_id,
        s.avg_play_ms_played,
        l.signup_date AS listener_signup_date,
        l.plan AS listener_plan,
        l.country AS listener_country
    FROM with_stats s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.country = 'KR'
),
play_source_stats AS (
    -- Avg avg_play_ms_played per play_source
    SELECT play_source, AVG(avg_play_ms_played) AS avg_avg_play_ms_played
    FROM with_details
    GROUP BY play_source
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_avg_play_ms_played,
        ROUND(s.avg_play_ms_played - g.avg_avg_play_ms_played, 2) AS avg_play_ms_played_minus_avg
    FROM with_details s
    JOIN play_source_stats g ON g.play_source = s.play_source
    WHERE s.avg_play_ms_played > g.avg_avg_play_ms_played
)
-- Final projection
SELECT
    id,
    listener_id,
    play_id,
    play_source,
    avg_play_ms_played,
    listener_signup_date,
    listener_plan,
    avg_play_ms_played_minus_avg,
    avg_avg_play_ms_played + avg_play_ms_played AS avg_avg_play_ms_played_plus_avg_play_ms_played
FROM vs_group
ORDER BY play_id, id DESC NULLS FIRST
