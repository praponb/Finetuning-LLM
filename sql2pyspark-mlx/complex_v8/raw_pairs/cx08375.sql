WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        ABS(ms_played - 130888) AS ms_played_distance,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-02-06 12:00:00'
),
grouped AS (
    -- Group-level totals
    SELECT
        source_len,
        SUM(CASE WHEN device = 'car' THEN 1 ELSE 0 END) AS n_device_car,
        COUNT(*) AS cnt
    FROM filtered_plays
    WHERE track_id IS NOT NULL
    GROUP BY 1
),
scored AS (
    -- Label and score rows
    SELECT *, ROUND(source_len / NULLIF(cnt, 0), 3) AS source_len_per_cnt
    FROM grouped
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM scored
    WHERE source_len_per_cnt > (SELECT MIN(source_len_per_cnt) FROM scored)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(cnt) OVER (PARTITION BY source_len, cnt), 2) AS source_len_cnt_avg_cnt,
        NTH_VALUE(n_device_car, 2) OVER (ORDER BY n_device_car ASC, source_len ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_device_car,
        LAST_VALUE(cnt) IGNORE NULLS OVER (ORDER BY n_device_car ASC, source_len ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cnt,
        ROUND(PERCENT_RANK() OVER w_all_by_n_device_car, 3) AS n_device_car_percent_rank,
        MAX(source_len) OVER (ORDER BY n_device_car ASC, source_len ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS source_len_max_4,
        ROUND(source_len_per_cnt - AVG(source_len_per_cnt) OVER (PARTITION BY source_len, cnt), 2) AS source_len_per_cnt_vs_source_len_cnt_avg
    FROM filtered
    WINDOW w_all_by_n_device_car AS (ORDER BY n_device_car ASC, source_len)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_trends
    WHERE source_len_per_cnt_vs_source_len_cnt_avg > (SELECT AVG(source_len_per_cnt_vs_source_len_cnt_avg) FROM with_trends)
)
-- Final projection
SELECT
    source_len,
    cnt,
    source_len_per_cnt,
    source_len_cnt_avg_cnt,
    second_n_device_car,
    last_known_cnt,
    n_device_car_percent_rank,
    source_len_cnt_avg_cnt + source_len_per_cnt AS source_len_cnt_avg_cnt_plus_source_len_per_cnt
FROM screened
