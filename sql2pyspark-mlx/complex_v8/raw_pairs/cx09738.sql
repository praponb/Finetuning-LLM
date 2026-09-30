WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label,
        ABS(voltage - 225.4) AS voltage_distance,
        UPPER(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE kwh > 45.3
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.read_at_label,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM cleaned_readings s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.quality IS NOT NULL
),
rollup_base AS (
    -- Summarise per group
    SELECT
        site_id,
        TO_DATE(reading_read_at) AS reading_read_at_day,
        ARRAY_SORT(COLLECT_SET(reading_quality)) AS reading_quality_list,
        COUNT(*) AS row_count,
        MIN(voltage) AS min_voltage,
        SUM(voltage) AS total_voltage,
        COUNT(DISTINCT reading_id) AS distinct_reading_ids
    FROM with_children_readings
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.site_id,
        s.reading_read_at_day,
        s.row_count,
        s.total_voltage,
        s.distinct_reading_ids,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality
    FROM rollup_base s
    JOIN meter_readings m ON m.site_id = s.site_id
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM exploded_detail_readings
    WHERE total_voltage > (SELECT AVG(total_voltage) FROM exploded_detail_readings)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(total_voltage) OVER (PARTITION BY row_count, site_id ORDER BY row_count ASC, site_id, reading_read_at_day, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_total_voltage,
        ROUND(total_voltage - AVG(total_voltage) OVER (PARTITION BY site_id), 2) AS total_voltage_vs_site_id_avg,
        SUM(reading_kwh) OVER (PARTITION BY site_id) AS site_id_sum_reading_kwh,
        ROW_NUMBER() OVER (PARTITION BY row_count, site_id ORDER BY row_count ASC, site_id, reading_read_at_day, reading_id) AS seq_no,
        RANK() OVER w_row_count_site_id_by_row_cou_2 AS row_count_rank,
        NTH_VALUE(distinct_reading_ids, 2) OVER (PARTITION BY row_count, site_id ORDER BY row_count ASC, site_id, reading_read_at_day, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_reading_ids
    FROM qualified
    WINDOW w_row_count_site_id_by_row_cou_2 AS (PARTITION BY row_count, site_id ORDER BY row_count ASC, site_id, reading_read_at_day, reading_id)
)
SELECT site_id, reading_read_at_day, field, amount
FROM (
    SELECT site_id, reading_read_at_day, site_id_sum_reading_kwh, total_voltage, reading_kwh
    FROM metrics
) src
UNPIVOT (
    amount FOR field IN (site_id_sum_reading_kwh, total_voltage, reading_kwh)
) unp
