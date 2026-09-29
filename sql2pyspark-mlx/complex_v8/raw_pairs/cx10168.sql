WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        UPPER(TRIM(source)) AS source_clean,
        CASE WHEN ms_played >= 246779 THEN 'A' WHEN ms_played >= 145720 THEN 'B' ELSE 'C' END AS ms_played_tier,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE device = 'car'
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        s.ms_played_filled,
        t.duration_sec AS track_duration_sec,
        t.title AS track_title,
        t.is_explicit AS track_is_explicit
    FROM cleaned_plays s
    LEFT JOIN tracks t ON t.id = s.track_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        ms_played_filled,
        track_title,
        track_is_explicit,
        NTH_VALUE(ms_played_filled, 2) OVER (PARTITION BY is_skipped ORDER BY ms_played_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_filled,
        ROUND(AVG(ms_played_filled) OVER (PARTITION BY source), 2) AS source_avg_ms_played_filled,
        FIRST_VALUE(track_duration_sec) IGNORE NULLS OVER (PARTITION BY is_skipped ORDER BY ms_played_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_track_duration_sec,
        ROW_NUMBER() OVER (ORDER BY ms_played_filled DESC, id) AS rn,
        NTILE(5) OVER (ORDER BY ms_played_filled DESC, id) AS ms_played_filled_quintile,
        ROUND(ms_played_filled - AVG(ms_played_filled) OVER w_source, 2) AS ms_played_filled_vs_source_avg
    FROM with_parent
    WINDOW w_source AS (PARTITION BY source)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(listener_id) OVER w_rn_device) AS distinct_listener_id_in_rn_device,
        ROUND(AVG(rn) OVER (PARTITION BY ms_played_filled_quintile ORDER BY rn DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS rn_avg_3
    FROM metrics
    WINDOW w_rn_device AS (PARTITION BY rn, device)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(ms_played_filled_quintile) OVER (PARTITION BY track_id ORDER BY distinct_listener_id_in_rn_device DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ms_played_filled_quintile_avg_4,
        LEAD(ms_played_filled) OVER (PARTITION BY device, ms_played_filled_quintile ORDER BY rn DESC, id) AS next_ms_played_filled,
        RANK() OVER (PARTITION BY is_skipped, distinct_listener_id_in_rn_device ORDER BY second_ms_played_filled DESC, id) AS second_ms_played_filled_rank,
        COUNT(*) OVER (PARTITION BY ms_played_filled_quintile) AS rows_in_ms_played_filled_quintile
    FROM ranked
)
SELECT
    id,
    listener_id,
    track_id,
    device,
    is_skipped,
    track_title,
    second_ms_played_filled,
    rn,
    ms_played_filled_vs_source_avg,
    rn_avg_3,
    ms_played_filled_quintile_avg_4,
    next_ms_played_filled,
    GREATEST(rn_avg_3, rn) AS greatest_rn_avg_3_rn
FROM sequenced
ORDER BY next_ms_played_filled, ms_played_filled_quintile_avg_4 DESC
