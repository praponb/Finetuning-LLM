WITH cleaned AS (
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
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label,
        DATE_TRUNC('HOUR', played_at) AS played_at_hour,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM plays
    WHERE HOUR(played_at) BETWEEN 10 AND 15
      AND is_skipped = TRUE
),
source_stats AS (
    -- Max ms_played per source
    SELECT source, MAX(ms_played) AS max_ms_played
    FROM cleaned
    GROUP BY source
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ms_played,
        ROUND(s.ms_played - g.max_ms_played, 2) AS ms_played_minus_max
    FROM cleaned s
    JOIN source_stats g ON g.source = s.source
    WHERE s.ms_played = g.max_ms_played
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        listener_id,
        source,
        MIN(played_at) AS first_played_at,
        SUM(CASE WHEN device IN ('speaker', 'phone') THEN 1 ELSE 0 END) AS n_device_in_speaker_phone
    FROM above_average
    WHERE played_at_label IS NOT NULL
    GROUP BY listener_id, source
    HAVING SUM(is_skipped_flag) > 0
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        listener_id,
        source,
        first_played_at,
        n_device_in_speaker_phone,
        NTH_VALUE(first_played_at, 2) OVER (ORDER BY n_device_in_speaker_phone, listener_id, source ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_played_at,
        MAX(n_device_in_speaker_phone) OVER (ORDER BY n_device_in_speaker_phone, listener_id, source ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS n_device_in_speaker_phone_max_4
    FROM rollup_base
),
combined_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.listener_id,
        s.source,
        s.first_played_at,
        s.n_device_in_speaker_phone,
        s.second_first_played_at,
        s.n_device_in_speaker_phone_max_4,
        l.interests AS listener_interests
    FROM windowed s
    JOIN listeners l ON l.id = s.listener_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(second_first_played_at) OVER (PARTITION BY source ORDER BY n_device_in_speaker_phone_max_4 DESC, listener_id, source ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_second_first_played_at,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY source, n_device_in_speaker_phone)) AS distinct_listener_id_in_source_n_device_in_speak,
        COUNT(*) OVER w_source_n_device_in_speaker_p AS rows_in_source_n_device_in_speaker_phone,
        ROUND(n_device_in_speaker_phone - AVG(n_device_in_speaker_phone) OVER w_source_n_device_in_speaker_p, 2) AS n_device_in_speaker_phone_vs_source_n_device_in_,
        ROW_NUMBER() OVER (PARTITION BY source ORDER BY n_device_in_speaker_phone_max_4 DESC, listener_id, source) AS rn
    FROM combined_details_listener
    WINDOW w_source_n_device_in_speaker_p AS (PARTITION BY source, n_device_in_speaker_phone)
)
SELECT
    listener_id,
    source,
    second_first_played_at,
    n_device_in_speaker_phone_max_4,
    last_second_first_played_at,
    rows_in_source_n_device_in_speaker_phone,
    n_device_in_speaker_phone_vs_source_n_device_in_,
    rn,
    LENGTH(TRIM(source)) AS source_len
FROM ranked
ORDER BY second_first_played_at DESC NULLS FIRST
