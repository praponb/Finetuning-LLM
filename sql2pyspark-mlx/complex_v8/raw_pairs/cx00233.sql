WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE kwh < 89.8
      AND voltage >= 213.7
),
per_group AS (
    -- Group-level totals
    SELECT
        YEAR(read_at) AS read_at_year,
        quality,
        site_id,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(voltage) AS min_voltage,
        SUM(CASE WHEN quality = 'EST' THEN voltage ELSE 0 END) AS voltage_quality_est,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        MIN(read_at) AS first_read_at
    FROM filtered
    GROUP BY 1, 2, 3
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        read_at_year,
        quality,
        site_id,
        distinct_site_ids,
        min_voltage,
        voltage_quality_est,
        first_read_at,
        MAX(distinct_ids) OVER (PARTITION BY site_id) AS site_id_max_distinct_ids,
        MAX(distinct_ids) OVER (PARTITION BY distinct_site_ids ORDER BY voltage_quality_est, read_at_year, quality, site_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS distinct_ids_max_1_prior,
        NTILE(5) OVER (PARTITION BY distinct_site_ids ORDER BY voltage_quality_est, read_at_year, quality, site_id) AS voltage_quality_est_quintile,
        NTILE(3) OVER (ORDER BY distinct_site_ids, read_at_year, quality, site_id) AS distinct_site_ids_tercile,
        SUM(read_at_year) OVER (PARTITION BY site_id) AS site_id_sum_read_at_year,
        ROUND(PERCENT_RANK() OVER (ORDER BY distinct_site_ids, read_at_year, quality, site_id), 3) AS distinct_site_ids_percent_rank
    FROM per_group
),
filtered_2 AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.tariff = 'FLAT'
              AND si.commissioned_on < DATE '2024-11-21'
        )
),
derived AS (
    -- Label and score rows
    SELECT *, LENGTH(TRIM(quality)) AS quality_len
    FROM filtered_2
),
filtered_3 AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality = 'OK'
              AND m.read_at >= TIMESTAMP '2025-02-06 09:00:00'
        )
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS min_reading_kwh
    FROM filtered_3 s
)
SELECT read_at_year, quality, metric, value
FROM (
    SELECT read_at_year, quality, min_voltage, distinct_site_ids_percent_rank
    FROM profiled
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (min_voltage AS m_min_voltage, distinct_site_ids_percent_rank AS m_distinct_site_ids_percent_rank)
) unp
