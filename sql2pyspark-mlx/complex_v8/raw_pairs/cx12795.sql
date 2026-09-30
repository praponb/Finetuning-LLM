WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        COALESCE(voltage, 0.0) AS voltage_filled,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix
    FROM meter_readings
    WHERE voltage < 232.4
      AND HOUR(read_at) BETWEEN 9 AND 15
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN m.quality IN ('OK', 'EST') THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count,
        (SELECT SUM(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_kwh,
        (SELECT MAX(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_voltage
    FROM scoped_readings s
),
stats AS (
    -- Group-level totals
    SELECT
        TO_DATE(read_at) AS read_at_day,
        SUM(voltage) AS total_voltage,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        MIN(read_at) AS first_read_at,
        ROUND(SUM(reading_quality_count) / NULLIF(SUM(kwh), 0), 3) AS reading_quality_count_to_kwh_ratio
    FROM summarized_rows
    WHERE quality_prefix IS NOT NULL
    GROUP BY TO_DATE(read_at)
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE distinct_site_ids > (SELECT AVG(distinct_site_ids) FROM stats)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM qualified
    WHERE distinct_site_ids > (SELECT AVG(distinct_site_ids) FROM qualified)
),
annotated AS (
    -- Add calculated columns
    SELECT
        read_at_day,
        total_voltage,
        distinct_site_ids,
        reading_quality_count_to_kwh_ratio,
        ROUND(total_voltage / NULLIF(distinct_site_ids, 0), 3) AS total_voltage_per_distinct_site_ids,
        distinct_site_ids - total_voltage AS distinct_site_ids_minus_total_voltage
    FROM without_matches
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY distinct_site_ids ORDER BY distinct_site_ids_minus_total_voltage DESC) AS distinct_site_ids_minus_total_voltage_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY distinct_site_ids ORDER BY distinct_site_ids_minus_total_voltage DESC, read_at_day) AS rn,
        COUNT(*) OVER w_distinct_site_ids AS rows_in_distinct_site_ids,
        SUM(distinct_site_ids) OVER (PARTITION BY distinct_site_ids ORDER BY distinct_site_ids_minus_total_voltage DESC, read_at_day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS distinct_site_ids_sum_3,
        DENSE_RANK() OVER w_distinct_site_ids_by_distinc_2 AS distinct_site_ids_minus_total_voltage_dense_rank_2,
        NTH_VALUE(distinct_site_ids, 2) OVER (ORDER BY total_voltage DESC, read_at_day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_site_ids
    FROM annotated
    WINDOW w_distinct_site_ids AS (PARTITION BY distinct_site_ids), w_distinct_site_ids_by_distinc_2 AS (PARTITION BY distinct_site_ids ORDER BY distinct_site_ids_minus_total_voltage DESC, read_at_day)
)
SELECT
    read_at_day,
    distinct_site_ids,
    total_voltage_per_distinct_site_ids,
    distinct_site_ids_minus_total_voltage,
    distinct_site_ids_minus_total_voltage_dense_rank,
    rn,
    rows_in_distinct_site_ids,
    distinct_site_ids_minus_total_voltage_dense_rank_2
FROM ranked
ORDER BY total_voltage_per_distinct_site_ids, read_at_day DESC
