WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('YEAR', read_at) AS read_at_year,
        kwh - voltage AS kwh_minus_voltage
    FROM meter_readings
    WHERE quality IS NOT NULL
      AND read_at < TIMESTAMP '2025-03-08 12:00:00'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(read_at_year, 2) OVER (PARTITION BY quality, site_id ORDER BY read_at_year NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at_year,
        DENSE_RANK() OVER (PARTITION BY quality, site_id ORDER BY read_at, id) AS read_at_dense_rank,
        NTILE(4) OVER (PARTITION BY quality, site_id ORDER BY read_at_year NULLS LAST, id) AS read_at_year_quartile,
        MAX(kwh_minus_voltage) OVER w_site_id_read_at_to_date AS site_id_read_at_to_date_max_kwh_minus_voltage,
        ROW_NUMBER() OVER w_site_id_by_kwh_minus_voltage AS seq_no,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality, site_id ORDER BY read_at), 3) AS read_at_percent_rank,
        ROUND(AVG(kwh) OVER (PARTITION BY quality, site_id ORDER BY read_at_year NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_4,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY quality, site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        COUNT(*) OVER (PARTITION BY quality, site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM valid_readings
    WINDOW w_site_id_read_at_to_date AS (PARTITION BY site_id, TO_DATE(read_at)), w_site_id_by_kwh_minus_voltage AS (PARTITION BY site_id ORDER BY kwh_minus_voltage DESC, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at_year,
        kwh_minus_voltage,
        second_read_at_year,
        read_at_dense_rank,
        read_at_year_quartile,
        site_id_read_at_to_date_max_kwh_minus_voltage,
        read_at_percent_rank,
        kwh_avg_4,
        second_voltage,
        running_count
    FROM with_trends
    WHERE seq_no = 1
      AND site_id_read_at_to_date_max_kwh_minus_voltage BETWEEN 547.5 AND 3285.0
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(read_at_dense_rank / NULLIF(SUM(read_at_dense_rank) OVER (PARTITION BY quality), 0), 4) AS read_at_dense_rank_share_of_quality,
        NTILE(5) OVER w_all_by_site_id_read_at_to_da AS site_id_read_at_to_date_max_kwh_minus_voltage_qu,
        RANK() OVER (PARTITION BY read_at_year_quartile, site_id ORDER BY kwh_avg_4 ASC, id) AS kwh_avg_4_rank,
        LAST_VALUE(read_at_year) OVER (PARTITION BY read_at_year_quartile, site_id ORDER BY kwh_avg_4 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at_year,
        MAX(read_at_percent_rank) OVER w_quality AS quality_max_read_at_percent_rank
    FROM leaders
    WINDOW w_all_by_site_id_read_at_to_da AS (ORDER BY site_id_read_at_to_date_max_kwh_minus_voltage ASC, id), w_quality AS (PARTITION BY quality)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality,
    read_at_year,
    second_read_at_year,
    read_at_dense_rank,
    read_at_percent_rank,
    kwh_avg_4,
    read_at_dense_rank_share_of_quality,
    site_id_read_at_to_date_max_kwh_minus_voltage_qu,
    last_read_at_year,
    ROUND(read_at_year_quartile / NULLIF(kwh_avg_4_rank, 0), 3) AS read_at_year_quartile_per_kwh_avg_4_rank
FROM ranked
ORDER BY second_read_at_year
