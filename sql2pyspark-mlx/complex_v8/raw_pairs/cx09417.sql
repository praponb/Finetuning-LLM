WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh > 14.4
),
combined_details_site AS (
    -- Attach site attributes
    SELECT
        s.*,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM recent_readings s
    JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(site_installed_kw) OVER (PARTITION BY site_id) AS site_id_sum_site_installed_kw,
        DENSE_RANK() OVER (ORDER BY kwh NULLS LAST, id) AS kwh_dense_rank,
        RANK() OVER (PARTITION BY site_id, quality ORDER BY kwh ASC NULLS LAST, id) AS kwh_rank,
        NTILE(4) OVER (PARTITION BY site_id, quality ORDER BY kwh ASC NULLS LAST, id) AS kwh_quartile,
        ROUND(AVG(voltage) OVER (ORDER BY kwh ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_7,
        COUNT(*) OVER (ORDER BY kwh NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(3) OVER w_all_by_kwh AS kwh_tercile,
        DENSE_RANK() OVER w_site_id_quality_by_kwh_ties AS kwh_dense_rank_2,
        ROUND(AVG(site_installed_kw) OVER (PARTITION BY site_id, quality ORDER BY kwh ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS site_installed_kw_avg_2,
        ROUND(CUME_DIST() OVER w_site_id_quality_by_kwh_ties, 3) AS kwh_cume_dist,
        NTILE(4) OVER (ORDER BY kwh NULLS LAST, id) AS kwh_quartile_3,
        FIRST_VALUE(read_at) OVER (ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id), 2) AS voltage_vs_site_id_avg,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id
    FROM combined_details_site
    WINDOW w_all_by_kwh AS (ORDER BY kwh NULLS LAST, id), w_site_id_quality_by_kwh_ties AS (PARTITION BY site_id, quality ORDER BY kwh ASC NULLS LAST)
),
top_rows AS (
    -- Top 3 per group
    SELECT *
    FROM metrics
    WHERE kwh_dense_rank_2 <= 3
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY quality, site_id ORDER BY site_commissioned_on, id) AS site_commissioned_on_rank,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_kwh_tercile_first_read_at_to, 0), 4) AS voltage_share_of_kwh_tercile_first_read_at_to_da,
        MIN(kwh_rank) OVER (PARTITION BY kwh_tercile, TO_DATE(first_read_at)) AS kwh_tercile_first_read_at_to_date_min_kwh_rank,
        NTH_VALUE(voltage_avg_7, 2) OVER (PARTITION BY kwh_quartile_3 ORDER BY kwh_dense_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage_avg_7,
        DENSE_RANK() OVER w_kwh_quartile_3_by_kwh_dense_2 AS kwh_dense_rank_dense_rank,
        COUNT(*) OVER (PARTITION BY kwh_tercile, TO_DATE(first_read_at)) AS rows_in_kwh_tercile_first_read_at_to_date,
        NTILE(5) OVER (PARTITION BY kwh_quartile_3 ORDER BY kwh_dense_rank DESC, id) AS kwh_dense_rank_quintile,
        NTILE(3) OVER w_kwh_quartile_3_by_kwh_dense_3 AS kwh_dense_rank_tercile
    FROM top_rows
    WINDOW w_kwh_tercile_first_read_at_to AS (PARTITION BY kwh_tercile, TO_DATE(first_read_at)), w_kwh_quartile_3_by_kwh_dense_2 AS (PARTITION BY kwh_quartile_3 ORDER BY kwh_dense_rank DESC), w_kwh_quartile_3_by_kwh_dense_3 AS (PARTITION BY kwh_quartile_3 ORDER BY kwh_dense_rank DESC, id)
)
SELECT
    id,
    read_at,
    kwh,
    quality,
    site_commissioned_on,
    site_installed_kw,
    site_name,
    site_id_sum_site_installed_kw,
    kwh_rank,
    kwh_quartile,
    voltage_avg_7,
    running_count,
    kwh_dense_rank_2,
    site_installed_kw_avg_2,
    kwh_cume_dist,
    first_read_at,
    rows_in_site_id,
    site_commissioned_on_rank,
    kwh_tercile_first_read_at_to_date_min_kwh_rank,
    second_voltage_avg_7,
    kwh_dense_rank_dense_rank,
    rows_in_kwh_tercile_first_read_at_to_date,
    kwh_dense_rank_tercile,
    ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(first_read_at)) / 60, 1) AS minutes_first_read_at_to_read_at,
    ROUND(kwh / NULLIF(rows_in_site_id, 0), 3) AS kwh_per_rows_in_site_id
FROM with_trends
WHERE kwh_rank > 3
ORDER BY quality DESC, kwh_dense_rank_2 DESC, id
LIMIT 5
