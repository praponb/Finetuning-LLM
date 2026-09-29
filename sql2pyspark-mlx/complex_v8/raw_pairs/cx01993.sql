WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT(source, '#', CAST(listener_id AS STRING)) AS source_ref,
        played_at + INTERVAL 1 DAYS AS played_at_plus_1d
    FROM plays
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.source_ref,
        t.duration_sec AS track_duration_sec,
        t.moods AS track_moods
    FROM filtered s
    JOIN tracks t ON t.id = s.track_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        source_ref,
        track_duration_sec,
        track_moods,
        NTH_VALUE(track_duration_sec, 2) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_track_duration_sec,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY listener_id ORDER BY ms_played, id))) / 60, 1) AS minutes_since_prev_played_at,
        ROUND(AVG(ms_played) OVER (PARTITION BY source, track_id ORDER BY ms_played ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_7,
        ROW_NUMBER() OVER w_played_at_month_track_id_by AS rn,
        LAST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at,
        SIZE(COLLECT_SET(listener_id) OVER w_track_id) AS distinct_listener_id_in_track_id,
        LAST_VALUE(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        ROW_NUMBER() OVER w_source_track_id_by_ms_played_2 AS ms_played_seq,
        NTILE(10) OVER (PARTITION BY source, track_id ORDER BY ms_played ASC NULLS LAST, id) AS ms_played_decile,
        NTH_VALUE(track_duration_sec, 2) OVER (ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_track_duration_sec_2,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        ROUND(track_duration_sec - AVG(track_duration_sec) OVER (PARTITION BY is_skipped), 2) AS track_duration_sec_vs_is_skipped_avg
    FROM with_details
    WINDOW w_played_at_month_track_id_by AS (PARTITION BY MONTH(played_at), track_id ORDER BY track_duration_sec, id), w_track_id AS (PARTITION BY track_id), w_source_track_id_by_ms_played_2 AS (PARTITION BY source, track_id ORDER BY ms_played ASC NULLS LAST, id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM metrics
    WHERE rn = 1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        second_track_duration_sec,
        minutes_since_prev_played_at,
        rn,
        last_known_played_at,
        last_ms_played,
        ms_played_seq,
        second_track_duration_sec_2,
        rows_in_track_id,
        track_duration_sec_vs_is_skipped_avg,
        MAX(ms_played) OVER (ORDER BY track_duration_sec_vs_is_skipped_avg DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS ms_played_max_1_prior,
        LAST_VALUE(rows_in_track_id) IGNORE NULLS OVER (PARTITION BY YEAR(last_known_played_at) ORDER BY minutes_since_prev_played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rows_in_track_id,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id ORDER BY track_duration_sec, id), 3) AS track_duration_sec_cume_dist,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY track_id), 0), 4) AS ms_played_share_of_track_id,
        ROUND(AVG(ms_played_avg_7) OVER (PARTITION BY track_id ORDER BY track_duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_7_avg_2_prior,
        ROUND(last_ms_played / NULLIF(SUM(last_ms_played) OVER (PARTITION BY track_id), 0), 4) AS last_ms_played_share_of_track_id,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY track_id)) AS distinct_listener_id_in_track_id_2,
        ROUND(AVG(ms_played_seq) OVER w_is_skipped, 2) AS is_skipped_avg_ms_played_seq
    FROM first_rows
    WINDOW w_is_skipped AS (PARTITION BY is_skipped)
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    is_skipped,
    second_track_duration_sec,
    rn,
    last_known_played_at,
    last_ms_played,
    ms_played_seq,
    second_track_duration_sec_2,
    rows_in_track_id,
    track_duration_sec_vs_is_skipped_avg,
    ms_played_max_1_prior,
    last_known_rows_in_track_id,
    track_duration_sec_cume_dist,
    ms_played_share_of_track_id,
    last_ms_played_share_of_track_id,
    distinct_listener_id_in_track_id_2,
    CASE WHEN rows_in_track_id >= 16 THEN 'HIGH' WHEN rows_in_track_id >= 8 THEN 'MEDIUM' ELSE 'LOW' END AS rows_in_track_id_tier,
    ms_played - ms_played_max_1_prior AS ms_played_minus_ms_played_max_1_prior
FROM sequenced
ORDER BY last_ms_played_share_of_track_id, id
LIMIT 10
