WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        QUARTER(read_at) AS read_at_quarter,
        LEAST(voltage, kwh) AS least_voltage_kwh
    FROM meter_readings
    WHERE voltage > 222.6
),
grouped AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        read_at_quarter,
        quality,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        COUNT(*) AS n_rows,
        MIN(least_voltage_kwh) AS min_least_voltage_kwh,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(read_at_quarter) AS total_read_at_quarter,
        ROUND(SUM(least_voltage_kwh) / NULLIF(SUM(voltage), 0), 3) AS least_voltage_kwh_to_voltage_ratio
    FROM cleaned_readings
    WHERE least_voltage_kwh > 85.8
    GROUP BY 1, 2, 3
    HAVING SUM(least_voltage_kwh) > 24.5
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        read_at_month,
        read_at_quarter,
        quality,
        quality_list,
        n_rows,
        min_least_voltage_kwh,
        distinct_ids,
        total_read_at_quarter,
        ROW_NUMBER() OVER (ORDER BY least_voltage_kwh_to_voltage_ratio, read_at_month, read_at_quarter, quality) AS rn,
        FIRST_VALUE(n_rows) OVER (PARTITION BY quality, read_at_quarter ORDER BY least_voltage_kwh_to_voltage_ratio DESC, read_at_month, read_at_quarter, quality ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_n_rows
    FROM grouped
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        read_at_month,
        read_at_quarter,
        quality,
        quality_list,
        n_rows,
        min_least_voltage_kwh,
        distinct_ids,
        total_read_at_quarter,
        FIRST_VALUE(rn) IGNORE NULLS OVER (PARTITION BY n_rows ORDER BY min_least_voltage_kwh DESC, read_at_month, read_at_quarter, quality ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rn,
        RANK() OVER (PARTITION BY n_rows ORDER BY min_least_voltage_kwh DESC, read_at_month, read_at_quarter, quality) AS min_least_voltage_kwh_rank
    FROM windowed
)
SELECT *
FROM (
    SELECT first_known_rn, read_at_quarter, quality, min_least_voltage_kwh
    FROM ranked
) src
PIVOT (
    COUNT(min_least_voltage_kwh)
    FOR quality IN ('OK', 'BAD', 'EST')
)
ORDER BY first_known_rn, read_at_quarter
