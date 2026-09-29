WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM meter_readings
),
with_details AS (
    -- Attach site attributes
    SELECT s.*, si.installed_kw AS site_installed_kw
    FROM recent s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw BETWEEN 96.9 AND 386.6
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(site_installed_kw) OVER (ORDER BY kwh, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS site_installed_kw_max_7,
        FIRST_VALUE(voltage) OVER (PARTITION BY quality ORDER BY site_installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage,
        RANK() OVER w_site_id_by_voltage AS voltage_rank,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality, site_id), 0), 4) AS voltage_share_of_quality_site_id,
        NTILE(4) OVER (PARTITION BY site_id ORDER BY voltage DESC, id) AS voltage_quartile,
        DENSE_RANK() OVER (PARTITION BY MONTH(read_at), quality ORDER BY voltage ASC, id) AS voltage_dense_rank,
        SUM(site_installed_kw) OVER (PARTITION BY quality ORDER BY site_installed_kw, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS site_installed_kw_sum_3,
        DENSE_RANK() OVER (PARTITION BY quality ORDER BY site_installed_kw, id) AS site_installed_kw_dense_rank,
        RANK() OVER (PARTITION BY quality ORDER BY site_installed_kw, id) AS site_installed_kw_rank,
        MAX(site_installed_kw) OVER (PARTITION BY quality, site_id) AS quality_site_id_max_site_installed_kw,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality, site_id), 2) AS kwh_vs_quality_site_id_avg,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY TO_DATE(read_at)), 0), 4) AS kwh_share_of_read_at_to_date,
        NTILE(3) OVER w_read_at_month_quality_by_vol AS voltage_tercile,
        LEAD(site_installed_kw, 1, 0) OVER w_site_id_by_voltage AS next_site_installed_kw
    FROM with_details
    WINDOW w_site_id_by_voltage AS (PARTITION BY site_id ORDER BY voltage DESC, id), w_read_at_month_quality_by_vol AS (PARTITION BY MONTH(read_at), quality ORDER BY voltage ASC, id)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        voltage_tercile,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        COUNT(DISTINCT site_id) AS distinct_site_ids
    FROM sequenced
    GROUP BY voltage_tercile
    HAVING SUM(voltage_quartile) > 4
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        voltage_tercile,
        n_rows,
        distinct_site_ids,
        NTH_VALUE(distinct_ids, 2) OVER (PARTITION BY voltage_tercile ORDER BY distinct_ids ASC, voltage_tercile ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_ids,
        LAST_VALUE(distinct_site_ids) OVER (PARTITION BY voltage_tercile ORDER BY distinct_ids ASC, voltage_tercile ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_distinct_site_ids,
        DENSE_RANK() OVER (PARTITION BY voltage_tercile ORDER BY distinct_ids ASC, voltage_tercile) AS distinct_ids_dense_rank
    FROM rollup_base
)
SELECT
    voltage_tercile,
    distinct_site_ids,
    second_distinct_ids,
    distinct_ids_dense_rank
FROM with_trends
ORDER BY distinct_site_ids DESC
