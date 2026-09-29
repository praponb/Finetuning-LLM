WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label,
        YEAR(read_at) AS read_at_year
    FROM meter_readings
    WHERE MONTH(read_at) IN (1, 2)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        read_at_year,
        NTH_VALUE(read_at_year, 2) OVER (PARTITION BY read_at_year ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at_year,
        COUNT(*) OVER w_read_at_year_site_id AS rows_in_read_at_year_site_id,
        ROUND(AVG(voltage) OVER w_read_at_year_site_id, 2) AS read_at_year_site_id_avg_voltage,
        LEAD(read_at_year) OVER (PARTITION BY site_id ORDER BY read_at_year DESC, id) AS next_read_at_year,
        ROUND(AVG(kwh) OVER (PARTITION BY YEAR(read_at), site_id), 2) AS read_at_year_site_id_avg_kwh,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY YEAR(read_at), site_id), 0), 4) AS voltage_share_of_read_at_year_site_id,
        MAX(voltage) OVER w_read_at_year_site_id AS read_at_year_site_id_max_voltage,
        RANK() OVER (PARTITION BY site_id ORDER BY read_at_year DESC, id) AS read_at_year_rank,
        DENSE_RANK() OVER (PARTITION BY site_id ORDER BY kwh DESC NULLS FIRST) AS kwh_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, YEAR(read_at) ORDER BY read_at DESC), 3) AS read_at_cume_dist,
        SUM(voltage) OVER w_read_at_year_site_id AS read_at_year_site_id_sum_voltage,
        NTILE(10) OVER w_site_id_read_at_year_by_read_2 AS read_at_decile
    FROM cleaned_readings
    WINDOW w_read_at_year_site_id AS (PARTITION BY YEAR(read_at), site_id), w_site_id_read_at_year_by_read_2 AS (PARTITION BY site_id, YEAR(read_at) ORDER BY read_at DESC, id)
),
stats AS (
    -- Group-level totals
    SELECT
        read_at_year,
        kwh_dense_rank,
        ROUND(STDDEV(voltage), 2) AS stddev_voltage,
        PERCENTILE_APPROX(voltage, 0.5) AS median_voltage
    FROM sequenced
    GROUP BY read_at_year, kwh_dense_rank
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(kwh_dense_rank) OVER w_read_at_year, 2) AS read_at_year_avg_kwh_dense_rank,
        MIN(kwh_dense_rank) OVER (PARTITION BY kwh_dense_rank, read_at_year ORDER BY stddev_voltage DESC, read_at_year, kwh_dense_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_dense_rank_so_far
    FROM stats
    WINDOW w_read_at_year AS (PARTITION BY read_at_year)
)
-- Final projection
SELECT
    read_at_year,
    kwh_dense_rank,
    median_voltage,
    min_kwh_dense_rank_so_far
FROM ranked
ORDER BY median_voltage DESC, read_at_year NULLS LAST
