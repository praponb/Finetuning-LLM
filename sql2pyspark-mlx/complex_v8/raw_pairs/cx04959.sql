WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-02-06 12:00:00'
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality,
        si.site_name
    FROM base s
    JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality ORDER BY kwh DESC), 3) AS kwh_percent_rank,
        RANK() OVER (PARTITION BY quality ORDER BY kwh DESC) AS kwh_rank,
        LAST_VALUE(voltage) OVER (PARTITION BY quality ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage,
        MIN(kwh) OVER (PARTITION BY quality, site_id) AS quality_site_id_min_kwh,
        COUNT(*) OVER w_quality_site_id AS rows_in_quality_site_id,
        RANK() OVER w_all_by_kwh_ties AS kwh_rank_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY voltage ASC, id), 3) AS voltage_percent_rank,
        LAST_VALUE(kwh) OVER (PARTITION BY site_id ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY quality ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        ROW_NUMBER() OVER w_all_by_kwh AS kwh_seq,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality, site_id), 2) AS kwh_vs_quality_site_id_avg,
        MIN(voltage) OVER (PARTITION BY quality, site_id) AS quality_site_id_min_voltage
    FROM with_parent_site
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id), w_all_by_kwh_ties AS (ORDER BY kwh DESC), w_all_by_kwh AS (ORDER BY kwh DESC, id)
)
SELECT
    id,
    kwh,
    voltage,
    quality,
    site_name,
    kwh_rank_2,
    last_kwh,
    kwh_seq,
    kwh_vs_quality_site_id_avg,
    quality_site_id_min_voltage,
    LOWER(REGEXP_REPLACE(TRIM(site_name), ' ', '_')) AS site_name_slug,
    ABS(kwh_seq - 5) AS kwh_seq_distance
FROM metrics
ORDER BY kwh_rank_2 DESC, site_name_slug
