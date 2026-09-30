WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        CASE WHEN quality IN ('OK', 'BAD') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE kwh >= 52.5
      AND site_id >= 5
),
with_parent AS (
    -- Join sites
    SELECT s.*, si.site_name, si.commissioned_on AS site_commissioned_on
    FROM recent s
    JOIN sites si ON si.id = s.site_id
    WHERE LOWER(si.site_name) LIKE '%ro%'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id), 2) AS voltage_vs_site_id_avg,
        SUM(voltage) OVER (ORDER BY read_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS voltage_sum_6_prior,
        SIZE(COLLECT_SET(quality_group) OVER w_site_id) AS distinct_quality_group_in_site_id,
        DENSE_RANK() OVER (PARTITION BY site_id ORDER BY voltage NULLS LAST) AS voltage_dense_rank,
        SUM(voltage) OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        RANK() OVER w_site_id_by_voltage_ties_2 AS voltage_rank,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        NTILE(3) OVER w_read_at_year_quality_group_b AS kwh_tercile,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_quality_group, 0), 4) AS kwh_share_of_quality_group
    FROM with_parent
    WINDOW w_site_id AS (PARTITION BY site_id), w_site_id_by_voltage_ties_2 AS (PARTITION BY site_id ORDER BY voltage DESC), w_read_at_year_quality_group_b AS (PARTITION BY YEAR(read_at), quality_group ORDER BY kwh DESC, id), w_quality_group AS (PARTITION BY quality_group)
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality_group,
        site_name,
        site_commissioned_on,
        voltage_vs_site_id_avg,
        voltage_sum_6_prior,
        distinct_quality_group_in_site_id,
        voltage_dense_rank,
        running_voltage,
        rows_in_site_id,
        kwh_tercile,
        kwh_share_of_quality_group
    FROM ranked
    WHERE voltage_rank <= 2
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    site_name,
    voltage_vs_site_id_avg,
    voltage_sum_6_prior,
    distinct_quality_group_in_site_id,
    running_voltage,
    rows_in_site_id,
    kwh_tercile,
    kwh_share_of_quality_group
FROM top_per_group
WHERE read_at < TIMESTAMP '2025-02-12 08:00:00'
