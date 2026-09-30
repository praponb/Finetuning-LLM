WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality NOT IN ('BAD', 'OK')
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        si.site_name,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM scoped s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw >= 141.6
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality,
        NTILE(3) OVER (PARTITION BY site_zone, site_id ORDER BY read_at DESC, id) AS read_at_tercile,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_site_zone, 0), 4) AS kwh_share_of_site_zone,
        MIN(kwh) OVER (ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_so_far,
        COUNT(*) OVER (PARTITION BY site_zone) AS rows_in_site_zone,
        SUM(kwh) OVER (PARTITION BY quality) AS quality_sum_kwh,
        DENSE_RANK() OVER (PARTITION BY site_zone, site_id ORDER BY read_at NULLS LAST, id) AS read_at_dense_rank,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER (PARTITION BY site_zone), 0), 4) AS site_installed_kw_share_of_site_zone,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        ROUND(site_installed_kw - AVG(site_installed_kw) OVER (PARTITION BY quality), 2) AS site_installed_kw_vs_quality_avg,
        NTH_VALUE(site_installed_kw, 2) OVER (ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_site_installed_kw
    FROM joined
    WINDOW w_site_zone AS (PARTITION BY site_zone)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(quality_sum_kwh, read_at_tercile) AS greatest_quality_sum_kwh_read_at_tercile,
        COALESCE(min_kwh_so_far, 0.0) AS min_kwh_so_far_filled,
        GREATEST(min_kwh_so_far, read_at_tercile) AS greatest_min_kwh_so_far_read_at_tercile
    FROM with_windows
    WHERE read_at_dense_rank BETWEEN 4 AND 6
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        read_at_dense_rank,
        read_at_tercile,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_ym,
        ROUND(SUM(read_at_tercile) / NULLIF(SUM(rows_in_site_id), 0), 3) AS read_at_tercile_to_rows_in_site_id_ratio,
        COUNT(*) AS n_rows,
        PERCENTILE_APPROX(distinct_site_id_in_quality, 0.5) AS median_distinct_site_id_in_quality
    FROM annotated
    WHERE read_at_tercile > 4
    GROUP BY read_at_dense_rank, read_at_tercile, DATE_FORMAT(read_at, 'yyyy-MM')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_n_rows_by_median_distinct_si AS median_distinct_site_id_in_quality_dense_rank,
        ROUND(AVG(read_at_tercile) OVER (PARTITION BY read_at_tercile, n_rows), 2) AS read_at_tercile_n_rows_avg_read_at_tercile,
        ROUND(PERCENT_RANK() OVER w_median_distinct_site_id_in_q, 3) AS n_rows_percent_rank,
        ROW_NUMBER() OVER w_median_distinct_site_id_in_q_2 AS n_rows_seq
    FROM grouped
    WINDOW w_n_rows_by_median_distinct_si AS (PARTITION BY n_rows ORDER BY median_distinct_site_id_in_quality ASC, read_at_dense_rank, read_at_tercile, read_at_ym), w_median_distinct_site_id_in_q AS (PARTITION BY median_distinct_site_id_in_quality ORDER BY n_rows DESC), w_median_distinct_site_id_in_q_2 AS (PARTITION BY median_distinct_site_id_in_quality ORDER BY n_rows DESC, read_at_dense_rank, read_at_tercile, read_at_ym)
)
SELECT
    read_at_dense_rank,
    read_at_tercile,
    read_at_ym,
    read_at_tercile_to_rows_in_site_id_ratio,
    n_rows,
    median_distinct_site_id_in_quality,
    read_at_tercile_n_rows_avg_read_at_tercile,
    n_rows_percent_rank,
    n_rows_seq
FROM ranked
