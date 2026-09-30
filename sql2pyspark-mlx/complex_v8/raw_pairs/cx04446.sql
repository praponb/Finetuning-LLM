WITH filtered AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label
    FROM plays
    WHERE (track_id IS NOT NULL OR NOT is_skipped)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        device,
        LAG(ms_played_filled) OVER (ORDER BY played_at, id) AS prev_ms_played_filled,
        ROUND(CUME_DIST() OVER (ORDER BY played_at), 3) AS played_at_cume_dist,
        RANK() OVER w_all_by_played_at_2 AS played_at_rank,
        COUNT(*) OVER (PARTITION BY device) AS rows_in_device,
        LEAD(played_at) OVER (ORDER BY played_at, id) AS next_played_at,
        LAG(ms_played, 1, 0) OVER w_all_by_played_at AS prev_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY device), 2) AS ms_played_vs_device_avg,
        RANK() OVER (PARTITION BY source ORDER BY ms_played NULLS LAST, id) AS ms_played_rank
    FROM filtered
    WINDOW w_all_by_played_at_2 AS (ORDER BY played_at DESC, id), w_all_by_played_at AS (ORDER BY played_at, id)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        LEAST(rows_in_device, ms_played_rank) AS least_rows_in_device_ms_played_rank,
        LEAST(rows_in_device, played_at_rank) AS least_rows_in_device_played_at_rank,
        QUARTER(played_at) AS played_at_quarter
    FROM with_windows
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        device,
        prev_ms_played_filled,
        played_at_cume_dist,
        played_at_rank,
        rows_in_device,
        ms_played_vs_device_avg,
        ms_played_rank,
        least_rows_in_device_ms_played_rank,
        least_rows_in_device_played_at_rank,
        played_at_quarter,
        NTILE(3) OVER (ORDER BY ms_played_rank, id) AS ms_played_rank_tercile,
        NTILE(4) OVER (PARTITION BY device, track_id ORDER BY prev_ms_played DESC, id) AS prev_ms_played_quartile,
        ROUND(played_at_cume_dist - AVG(played_at_cume_dist) OVER (PARTITION BY ms_played_rank, MONTH(next_played_at)), 2) AS played_at_cume_dist_vs_ms_played_rank_next_playe,
        SUM(played_at_quarter) OVER (PARTITION BY device, track_id ORDER BY prev_ms_played DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS played_at_quarter_sum_1_prior,
        LEAD(next_played_at) OVER (ORDER BY ms_played_rank, id) AS next_next_played_at,
        ROUND(AVG(prev_ms_played_filled) OVER (PARTITION BY ms_played_rank, MONTH(next_played_at)), 2) AS ms_played_rank_next_played_at_month_avg_prev_ms_,
        ROW_NUMBER() OVER (ORDER BY ms_played_rank, id) AS rn
    FROM scored
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        device,
        played_at_rank,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        MAX(played_at_cume_dist_vs_ms_played_rank_next_playe) AS max_played_at_cume_dist_vs_ms_played_rank_next_p,
        PERCENTILE_APPROX(ms_played_rank, 0.5) AS median_ms_played_rank
    FROM sequenced
    GROUP BY 1, 2
)
SELECT
    device,
    played_at_rank,
    device_list,
    max_played_at_cume_dist_vs_ms_played_rank_next_p,
    LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
FROM totals
ORDER BY played_at_rank DESC, device
