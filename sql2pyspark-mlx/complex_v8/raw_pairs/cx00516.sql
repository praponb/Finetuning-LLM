WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE SIZE(moods) >= 1
),
unique_rows AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        duration_sec_per_bpm,
        title_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm, id) AS recency_rank
        FROM prepared
    ) ranked
    WHERE recency_rank = 1
),
unique_rows_2 AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        duration_sec_per_bpm,
        title_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id DESC) AS recency_rank
        FROM unique_rows
    ) ranked
    WHERE recency_rank = 1
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (ORDER BY duration_sec_per_bpm, id) AS duration_sec_per_bpm_quartile,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_id), 0), 4) AS duration_sec_share_of_artist_id,
        NTH_VALUE(title_len, 2) OVER (ORDER BY duration_sec_per_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_title_len,
        ROUND(PERCENT_RANK() OVER w_all_by_duration_sec_per_bpm_3, 3) AS duration_sec_per_bpm_percent_rank
    FROM unique_rows_2
    WINDOW w_all_by_duration_sec_per_bpm_3 AS (ORDER BY duration_sec_per_bpm)
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.played_at < TIMESTAMP '2025-02-18 10:00:00'
              AND p.listener_id <= 6
        )
      AND s.is_explicit = TRUE
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.duration_sec_per_bpm,
        s.duration_sec_per_bpm_quartile,
        s.duration_sec_share_of_artist_id,
        s.duration_sec_per_bpm_percent_rank,
        p.id AS play_id,
        p.source AS play_source,
        p.device AS play_device,
        p.listener_id AS play_listener_id
    FROM with_activity s
    JOIN plays p ON p.track_id = s.id AND p.device <> 'speaker'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        duration_sec,
        duration_sec_per_bpm,
        duration_sec_per_bpm_quartile,
        play_id,
        play_source,
        play_listener_id,
        duration_sec_per_bpm - LAG(duration_sec_per_bpm) OVER (PARTITION BY duration_sec_per_bpm_quartile ORDER BY duration_sec_per_bpm_quartile, id, play_id) AS duration_sec_per_bpm_change,
        LAST_VALUE(duration_sec_per_bpm) IGNORE NULLS OVER (PARTITION BY duration_sec_per_bpm_quartile ORDER BY duration_sec_share_of_artist_id, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec_per_bpm,
        SIZE(COLLECT_SET(play_device) OVER (PARTITION BY play_listener_id)) AS distinct_play_device_in_play_listener_id,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY duration_sec_per_bpm_quartile ORDER BY duration_sec_per_bpm_quartile, id, play_id) AS duration_sec_change,
        LEAD(duration_sec_per_bpm, 3) OVER (PARTITION BY duration_sec_per_bpm_quartile ORDER BY duration_sec_per_bpm_quartile, id, play_id) AS next3_duration_sec_per_bpm
    FROM detail_rows
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    duration_sec_per_bpm,
    duration_sec_per_bpm_quartile,
    play_source,
    play_listener_id,
    duration_sec_per_bpm_change,
    last_known_duration_sec_per_bpm,
    distinct_play_device_in_play_listener_id,
    duration_sec_change,
    SUBSTRING(TRIM(play_source), 1, 3) AS play_source_prefix
FROM metrics
