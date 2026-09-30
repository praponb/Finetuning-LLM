WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LEAST(voltage, kwh) AS least_voltage_kwh,
        ABS(kwh - 50.4) AS kwh_distance,
        DATE_TRUNC('YEAR', read_at) AS read_at_year
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MIN(read_at) AS first_reading_read_at,
        MAX(read_at) AS last_reading_read_at,
        MIN(read_at) AS first_reading_read_at_2
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at,
        m.last_reading_read_at,
        m.first_reading_read_at_2
    FROM prepared_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        kwh,
        least_voltage_kwh,
        kwh_distance,
        first_reading_read_at,
        last_reading_read_at,
        first_reading_read_at_2,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        DENSE_RANK() OVER (PARTITION BY YEAR(first_reading_read_at) ORDER BY last_reading_read_at ASC) AS last_reading_read_at_dense_rank,
        LAST_VALUE(read_at_year) OVER (PARTITION BY YEAR(first_reading_read_at) ORDER BY last_reading_read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at_year
    FROM summarized_rows
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.zone AS site_zone,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw
    FROM metrics s
    JOIN sites si ON si.id = s.site_id
),
metrics_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        kwh_distance,
        last_reading_read_at,
        first_reading_read_at_2,
        rows_in_site_id,
        last_read_at_year,
        site_zone,
        site_installed_kw,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id, site_tariff ORDER BY last_reading_read_at_dense_rank), 3) AS last_reading_read_at_dense_rank_percent_rank,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY site_id, site_tariff ORDER BY last_reading_read_at_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        LEAD(first_reading_read_at_2) OVER (PARTITION BY site_id, site_tariff ORDER BY last_reading_read_at_dense_rank, id) AS next_first_reading_read_at_2,
        NTILE(3) OVER w_site_id_site_tariff_by_last_3 AS last_reading_read_at_dense_rank_tercile,
        FIRST_VALUE(first_reading_read_at) IGNORE NULLS OVER (PARTITION BY site_id, site_tariff ORDER BY last_reading_read_at_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_reading_read_at,
        SUM(rows_in_site_id) OVER (PARTITION BY site_id, site_tariff ORDER BY last_reading_read_at_dense_rank, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS rows_in_site_id_sum_6_prior
    FROM joined
    WINDOW w_site_id_site_tariff_by_last_3 AS (PARTITION BY site_id, site_tariff ORDER BY last_reading_read_at_dense_rank, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(site_installed_kw, 2) OVER (ORDER BY last_reading_read_at_dense_rank_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_site_installed_kw,
        SUM(kwh_distance) OVER (ORDER BY last_reading_read_at_dense_rank_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh_distance
    FROM metrics_2
),
metrics_3 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(second_site_installed_kw - AVG(second_site_installed_kw) OVER (PARTITION BY MONTH(first_reading_read_at_2)), 2) AS second_site_installed_kw_vs_first_reading_read_a,
        NTH_VALUE(last_reading_read_at, 2) OVER (ORDER BY last_reading_read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_reading_read_at
    FROM with_windows
)
SELECT id, field, value
FROM (
    SELECT id, last_reading_read_at_dense_rank_tercile, rows_in_site_id
    FROM metrics_3
) src
UNPIVOT (
    value FOR field IN (last_reading_read_at_dense_rank_tercile AS last_reading_read_at_dense_rank_tercile_v, rows_in_site_id AS m_rows_in_site_id)
) unp
