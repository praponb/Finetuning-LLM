WITH valid_plays AS (
    -- Filter plays
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
    WHERE NOT is_skipped
),
combined_details AS (
    -- Enrich with dimension data
    SELECT s.*, l.plan AS listener_plan
    FROM valid_plays s
    JOIN listeners l ON l.id = s.listener_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_7,
        MIN(ms_played) OVER (PARTITION BY track_id, source) AS track_id_source_min_ms_played,
        MAX(ms_played) OVER (PARTITION BY track_id) AS track_id_max_ms_played,
        LAG(played_at, 2) OVER (PARTITION BY device, track_id ORDER BY played_at ASC, id) AS prev2_played_at,
        NTILE(3) OVER (ORDER BY played_at ASC, id) AS played_at_tercile,
        SIZE(COLLECT_SET(listener_plan) OVER w_track_id) AS distinct_listener_plan_in_track_id,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY track_id, source), 0), 4) AS ms_played_share_of_track_id_source,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_listener_id_device, 0), 4) AS ms_played_share_of_listener_id_device,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY track_id)) AS distinct_source_in_track_id,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY listener_id, device)) AS distinct_track_id_in_listener_id_device,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        DENSE_RANK() OVER (PARTITION BY listener_plan, source ORDER BY played_at ASC) AS played_at_dense_rank,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id))) / 60, 1) AS minutes_since_prev_played_at,
        LEAD(played_at) OVER (PARTITION BY device ORDER BY played_at ASC NULLS LAST, id) AS next_played_at,
        MAX(ms_played) OVER (PARTITION BY listener_plan, source ORDER BY played_at ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ms_played_max_6_prior
    FROM combined_details
    WINDOW w_track_id AS (PARTITION BY track_id), w_listener_id_device AS (PARTITION BY listener_id, device)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        played_at_dense_rank,
        YEAR(prev2_played_at) AS prev2_played_at_year,
        PERCENTILE_APPROX(played_at_dense_rank, 0.5) AS median_played_at_dense_rank,
        ARRAY_SORT(COLLECT_SET(listener_plan)) AS listener_plan_list,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        COUNT(DISTINCT track_id) AS distinct_track_ids,
        COUNT(DISTINCT id) AS distinct_ids
    FROM windowed
    WHERE ms_played_share_of_track_id_source < 0.6
    GROUP BY DATE_TRUNC('MONTH', played_at), played_at_dense_rank, YEAR(prev2_played_at)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        distinct_ids - LAG(distinct_ids) OVER (PARTITION BY prev2_played_at_year, median_played_at_dense_rank ORDER BY distinct_ids ASC, played_at_month, played_at_dense_rank, prev2_played_at_year) AS distinct_ids_change,
        ROUND(median_played_at_dense_rank - AVG(median_played_at_dense_rank) OVER w_n_rows_distinct_track_ids, 2) AS median_played_at_dense_rank_vs_n_rows_distinct_t
    FROM rollup_base
    WINDOW w_n_rows_distinct_track_ids AS (PARTITION BY n_rows, distinct_track_ids)
)
SELECT
    played_at_month,
    prev2_played_at_year,
    median_played_at_dense_rank,
    n_rows,
    device_list,
    distinct_track_ids,
    distinct_ids,
    median_played_at_dense_rank_vs_n_rows_distinct_t
FROM sequenced
ORDER BY played_at_month NULLS LAST, prev2_played_at_year
