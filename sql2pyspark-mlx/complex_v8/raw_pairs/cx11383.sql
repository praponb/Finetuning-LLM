WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE ms_played >= 99376
      AND listener_id IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(ms_played) OVER (PARTITION BY listener_id) AS listener_id_max_ms_played,
        NTILE(3) OVER (ORDER BY ms_played, id) AS ms_played_tercile,
        SUM(ms_played) OVER (ORDER BY played_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ms_played_sum_3,
        COUNT(*) OVER (PARTITION BY listener_id) AS rows_in_listener_id,
        DENSE_RANK() OVER (ORDER BY ms_played, id) AS ms_played_dense_rank,
        NTILE(5) OVER (ORDER BY ms_played, id) AS ms_played_quintile,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (ORDER BY ms_played DESC, id))) / 60, 1) AS minutes_since_prev_played_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY ms_played), 3) AS ms_played_percent_rank,
        NTH_VALUE(played_at, 2) OVER (ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROUND(AVG(ms_played) OVER (ORDER BY played_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_2_prior,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY ms_played DESC, id), 3) AS ms_played_percent_rank_2,
        SUM(ms_played) OVER (ORDER BY ms_played DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ms_played_sum_3_prior,
        ROUND(AVG(ms_played) OVER (ORDER BY ms_played DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2
    FROM filtered_plays
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        listener_id_max_ms_played,
        ms_played_tercile,
        ms_played_sum_3,
        rows_in_listener_id,
        ms_played_quintile,
        ms_played_percent_rank,
        ms_played_avg_2_prior,
        ms_played_percent_rank_2,
        ms_played_sum_3_prior,
        ms_played_avg_2,
        INITCAP(TRIM(device)) AS device_clean
    FROM sequenced
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        listener_id_max_ms_played,
        ms_played_sum_3,
        rows_in_listener_id,
        ms_played_avg_2_prior,
        ms_played_percent_rank_2,
        ms_played_sum_3_prior,
        ms_played_avg_2,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY ms_played_quintile)) AS distinct_track_id_in_ms_played_quintile,
        SUM(rows_in_listener_id) OVER (ORDER BY rows_in_listener_id ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rows_in_listener_id_sum_3,
        SUM(ms_played_quintile) OVER (PARTITION BY is_skipped ORDER BY ms_played_avg_2, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ms_played_quintile_sum_2_prior,
        FIRST_VALUE(rows_in_listener_id) IGNORE NULLS OVER (ORDER BY rows_in_listener_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rows_in_listener_id,
        MAX(ms_played) OVER (ORDER BY rows_in_listener_id ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_max_7,
        NTILE(3) OVER w_all_by_rows_in_listener_id AS rows_in_listener_id_tercile,
        MAX(listener_id_max_ms_played) OVER (PARTITION BY ms_played_quintile) AS ms_played_quintile_max_listener_id_max_ms_played,
        RANK() OVER (ORDER BY rows_in_listener_id ASC, id) AS rows_in_listener_id_rank
    FROM enriched
    WINDOW w_all_by_rows_in_listener_id AS (ORDER BY rows_in_listener_id ASC, id)
)
SELECT
    id,
    track_id,
    played_at,
    ms_played,
    is_skipped,
    rows_in_listener_id,
    ms_played_avg_2,
    distinct_track_id_in_ms_played_quintile,
    rows_in_listener_id_sum_3,
    ms_played_quintile_sum_2_prior,
    first_known_rows_in_listener_id,
    rows_in_listener_id_tercile,
    ms_played_quintile_max_listener_id_max_ms_played,
    rows_in_listener_id_rank,
    ABS(ms_played_percent_rank_2 - 0.7) AS ms_played_percent_rank_2_distance
FROM metrics
ORDER BY first_known_rows_in_listener_id DESC NULLS FIRST, ms_played_percent_rank_2_distance DESC, id
LIMIT 20
