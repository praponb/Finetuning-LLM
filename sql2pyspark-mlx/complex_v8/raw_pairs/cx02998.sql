WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh
    FROM meter_readings
    WHERE voltage > 222.3
),
site_id_stats AS (
    -- Max kwh per site_id
    SELECT site_id, MAX(kwh) AS max_kwh
    FROM base
    GROUP BY site_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_kwh, ROUND(s.kwh - g.max_kwh, 2) AS kwh_minus_max
    FROM base s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.kwh = g.max_kwh
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(read_at) AS read_at_day,
        COUNT(*) FILTER (WHERE quality IS NOT NULL) AS n_quality_is_not_null,
        MIN(max_kwh) AS min_max_kwh,
        ROUND(SUM(voltage_per_kwh), 2) AS total_voltage_per_kwh
    FROM vs_group
    GROUP BY 1
    HAVING MAX(voltage_per_kwh) > 2.5
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(n_quality_is_not_null) OVER (PARTITION BY YEAR(read_at_day), n_quality_is_not_null ORDER BY min_max_kwh, read_at_day) AS prev_n_quality_is_not_null,
        ROUND(min_max_kwh / NULLIF(SUM(min_max_kwh) OVER w_n_quality_is_not_null_read_a, 0), 4) AS min_max_kwh_share_of_n_quality_is_not_null_read_,
        SUM(n_quality_is_not_null) OVER (PARTITION BY n_quality_is_not_null, YEAR(read_at_day)) AS n_quality_is_not_null_read_at_day_year_sum_n_qua,
        ROUND(n_quality_is_not_null / NULLIF(SUM(n_quality_is_not_null) OVER (PARTITION BY n_quality_is_not_null, YEAR(read_at_day)), 0), 4) AS n_quality_is_not_null_share_of_n_quality_is_not_
    FROM rollup_base
    WINDOW w_n_quality_is_not_null_read_a AS (PARTITION BY n_quality_is_not_null, YEAR(read_at_day))
)
SELECT
    read_at_day,
    n_quality_is_not_null,
    total_voltage_per_kwh,
    prev_n_quality_is_not_null,
    min_max_kwh_share_of_n_quality_is_not_null_read_,
    n_quality_is_not_null_read_at_day_year_sum_n_qua,
    n_quality_is_not_null_share_of_n_quality_is_not_,
    min_max_kwh - total_voltage_per_kwh AS min_max_kwh_minus_total_voltage_per_kwh
FROM with_windows
WHERE prev_n_quality_is_not_null < 8
ORDER BY min_max_kwh_minus_total_voltage_per_kwh DESC
