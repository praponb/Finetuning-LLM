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
        LENGTH(TRIM(source)) AS source_len,
        CONCAT(source, '#', CAST(listener_id AS STRING)) AS source_ref
    FROM plays
    WHERE device IN ('phone', 'speaker')
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(source_len) OVER (PARTITION BY device ORDER BY ms_played DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS source_len_max_2,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        SIZE(COLLECT_SET(listener_id) OVER w_track_id) AS distinct_listener_id_in_track_id,
        DENSE_RANK() OVER (PARTITION BY device ORDER BY ms_played DESC) AS ms_played_dense_rank,
        ROUND(AVG(ms_played) OVER (PARTITION BY device ORDER BY ms_played DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_6_prior
    FROM cleaned
    WINDOW w_track_id AS (PARTITION BY track_id)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        device,
        is_skipped,
        source_len,
        source_len_max_2,
        rows_in_track_id,
        distinct_listener_id_in_track_id,
        ms_played_dense_rank,
        ms_played_avg_6_prior,
        LAG(ms_played_avg_6_prior) OVER (ORDER BY ms_played_avg_6_prior, id) AS prev_ms_played_avg_6_prior,
        ROUND(ms_played_avg_6_prior - AVG(ms_played_avg_6_prior) OVER w_distinct_listener_id_in_trac, 2) AS ms_played_avg_6_prior_vs_distinct_listener_id_in,
        MIN(ms_played_avg_6_prior) OVER (PARTITION BY distinct_listener_id_in_track_id) AS distinct_listener_id_in_track_id_min_ms_played_a,
        MIN(ms_played_dense_rank) OVER (PARTITION BY distinct_listener_id_in_track_id) AS distinct_listener_id_in_track_id_min_ms_played_d,
        ROW_NUMBER() OVER w_all_by_ms_played_avg_6_prior AS seq_no
    FROM sequenced
    WINDOW w_distinct_listener_id_in_trac AS (PARTITION BY distinct_listener_id_in_track_id), w_all_by_ms_played_avg_6_prior AS (ORDER BY ms_played_avg_6_prior, id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT distinct_listener_id_in_track_id, device, ms_played_avg_6_prior_vs_distinct_listener_id_in
    FROM windowed
) src
PIVOT (
    SUM(ms_played_avg_6_prior_vs_distinct_listener_id_in)
    FOR device IN ('tv', 'speaker', 'desktop', 'phone')
)
