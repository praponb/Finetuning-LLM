WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        COALESCE(kwh, 0.0) AS kwh_filled,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE voltage >= 213.7
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        kwh_filled,
        ROUND(kwh_filled / NULLIF(SUM(kwh_filled) OVER (PARTITION BY site_id), 0), 4) AS kwh_filled_share_of_site_id,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY site_id ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY read_at DESC), 3) AS read_at_cume_dist,
        SUM(kwh_filled) OVER (PARTITION BY site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh_filled,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id
    FROM filtered_readings
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        second_voltage - LAG(second_voltage) OVER (ORDER BY read_at_cume_dist DESC, id) AS second_voltage_change,
        DENSE_RANK() OVER w_all_by_read_at_cume_dist_tie AS read_at_cume_dist_dense_rank,
        LAG(running_kwh_filled, 3, 0) OVER (ORDER BY read_at_cume_dist DESC, id) AS prev3_running_kwh_filled,
        ROW_NUMBER() OVER (ORDER BY read_at_cume_dist DESC, id) AS rn,
        SUM(running_kwh_filled) OVER (ORDER BY read_at_cume_dist DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS running_kwh_filled_sum_2_prior,
        RANK() OVER (ORDER BY read_at_cume_dist DESC, id) AS read_at_cume_dist_rank
    FROM ranked
    WINDOW w_all_by_read_at_cume_dist_tie AS (ORDER BY read_at_cume_dist DESC)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label,
        FLOOR(running_kwh_filled / 250) * 250 AS running_kwh_filled_bucket
    FROM with_windows
    WHERE read_at_cume_dist <= 0.7
)
SELECT
    rn,
    read_at_cume_dist_dense_rank,
    GROUPING_ID() AS gid,
    MAX(read_at) AS last_read_at
FROM annotated
GROUP BY GROUPING SETS ((rn, read_at_cume_dist_dense_rank), (rn), ())
ORDER BY rn NULLS LAST, read_at_cume_dist_dense_rank NULLS LAST
