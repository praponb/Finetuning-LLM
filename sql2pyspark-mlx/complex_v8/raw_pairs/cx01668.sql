WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        ABS(voltage - 221.8) AS voltage_distance
    FROM meter_readings
    WHERE quality <> 'OK'
),
combined_details_site AS (
    -- Attach site attributes
    SELECT s.*, si.installed_kw AS site_installed_kw
    FROM recent_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER w_site_id_by_site_installed_kw AS site_installed_kw_seq,
        LAST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh,
        MIN(site_installed_kw) OVER (PARTITION BY site_id) AS site_id_min_site_installed_kw,
        ROUND(AVG(voltage_distance) OVER w_site_id, 2) AS site_id_avg_voltage_distance,
        LEAD(kwh, 1, 0) OVER (PARTITION BY quality, site_id ORDER BY voltage_distance DESC, id) AS next_kwh,
        NTILE(3) OVER (PARTITION BY quality, site_id ORDER BY voltage_distance DESC, id) AS voltage_distance_tercile,
        SUM(voltage_distance) OVER (PARTITION BY site_id, quality ORDER BY site_installed_kw, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS voltage_distance_sum_4,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY site_installed_kw DESC, id), 3) AS site_installed_kw_cume_dist
    FROM combined_details_site
    WINDOW w_site_id_by_site_installed_kw AS (PARTITION BY site_id ORDER BY site_installed_kw DESC, id), w_site_id AS (PARTITION BY site_id)
),
grouped AS (
    -- Summarise per group
    SELECT
        quality,
        site_id,
        site_installed_kw_seq,
        COUNT(*) AS n_rows,
        SUM(site_id_min_site_installed_kw) AS total_site_id_min_site_installed_kw,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        ROUND(SUM(last_known_kwh) / NULLIF(SUM(site_id_min_site_installed_kw), 0), 3) AS last_known_kwh_to_site_id_min_site_installed_kw_,
        SUM(CASE WHEN quality NOT IN ('EST', 'BAD') THEN site_id_avg_voltage_distance ELSE 0 END) AS site_id_avg_voltage_distance_quality_not_in_est_
    FROM windowed
    GROUP BY quality, site_id, site_installed_kw_seq
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        quality,
        site_id,
        site_installed_kw_seq,
        n_rows,
        quality_list,
        last_known_kwh_to_site_id_min_site_installed_kw_,
        ROUND(n_rows - AVG(n_rows) OVER (PARTITION BY site_id, n_rows), 2) AS n_rows_vs_site_id_n_rows_avg,
        ROUND(n_rows / NULLIF(SUM(n_rows) OVER (PARTITION BY site_id, n_rows), 0), 4) AS n_rows_share_of_site_id_n_rows,
        MIN(n_rows) OVER (PARTITION BY site_id, n_rows) AS site_id_n_rows_min_n_rows,
        ROUND(site_installed_kw_seq - AVG(site_installed_kw_seq) OVER w_site_id_n_rows, 2) AS site_installed_kw_seq_vs_site_id_n_rows_avg
    FROM grouped
    WINDOW w_site_id_n_rows AS (PARTITION BY site_id, n_rows)
)
SELECT
    quality,
    n_rows,
    quality_list,
    last_known_kwh_to_site_id_min_site_installed_kw_,
    n_rows_vs_site_id_n_rows_avg,
    n_rows_share_of_site_id_n_rows,
    site_id_n_rows_min_n_rows,
    site_installed_kw_seq_vs_site_id_n_rows_avg,
    SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix
FROM sequenced
ORDER BY last_known_kwh_to_site_id_min_site_installed_kw_ NULLS LAST, site_installed_kw_seq_vs_site_id_n_rows_avg
