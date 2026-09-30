WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
    WHERE kwh <= 92.5
),
site_id_stats AS (
    -- Median voltage per site_id
    SELECT site_id, PERCENTILE_APPROX(voltage, 0.5) AS median_voltage
    FROM recent_readings
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_voltage,
        ROUND(s.voltage - g.median_voltage, 2) AS voltage_minus_median
    FROM recent_readings s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.voltage >= g.median_voltage
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY MONTH(read_at), site_id ORDER BY median_voltage DESC) AS median_voltage_dense_rank,
        ROUND(AVG(voltage_minus_median) OVER (PARTITION BY MONTH(read_at), site_id ORDER BY median_voltage DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS voltage_minus_median_avg_2,
        NTILE(5) OVER w_read_at_month_site_id_by_med_3 AS median_voltage_quintile,
        kwh - LAG(kwh) OVER (PARTITION BY quality, TO_DATE(read_at) ORDER BY voltage_minus_median ASC, id) AS kwh_change
    FROM above_average
    WINDOW w_read_at_month_site_id_by_med_3 AS (PARTITION BY MONTH(read_at), site_id ORDER BY median_voltage DESC, id)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        kwh,
        median_voltage_dense_rank,
        voltage_minus_median_avg_2,
        kwh_change,
        ROUND(kwh - AVG(kwh) OVER w_quality, 2) AS kwh_vs_quality_avg,
        ROUND(kwh_change - AVG(kwh_change) OVER w_quality, 2) AS kwh_change_vs_quality_avg,
        COUNT(*) OVER w_quality AS rows_in_quality
    FROM windowed
    WINDOW w_quality AS (PARTITION BY quality)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(rows_in_quality, 2) OVER (ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rows_in_quality,
        LAST_VALUE(kwh) OVER (ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        NTILE(10) OVER (ORDER BY rows_in_quality DESC, id) AS rows_in_quality_decile,
        LAST_VALUE(median_voltage_dense_rank) IGNORE NULLS OVER (ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_median_voltage_dense_rank
    FROM sequenced
)
SELECT id, field, reading
FROM (
    SELECT id, second_rows_in_quality, median_voltage_dense_rank, rows_in_quality_decile, rows_in_quality
    FROM with_windows
) src
UNPIVOT (
    reading FOR field IN (second_rows_in_quality AS m_second_rows_in_quality, median_voltage_dense_rank AS m_median_voltage_dense_rank, rows_in_quality_decile AS rows_in_quality_decile_v, rows_in_quality AS m_rows_in_quality)
) unp
