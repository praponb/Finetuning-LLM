WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        voltage - kwh AS voltage_minus_kwh
    FROM meter_readings
),
with_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.read_at_month,
        s.voltage_minus_kwh,
        si.tariff AS site_tariff
    FROM valid_readings s
    JOIN sites si ON si.id = s.site_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(read_at_month) - UNIX_TIMESTAMP(LAG(read_at_month) OVER w_site_id_quality_by_read_at_m)) / 60, 1) AS minutes_since_prev_read_at_month,
        COUNT(*) OVER w_read_at_year_site_id AS rows_in_read_at_year_site_id,
        LEAD(read_at, 2) OVER (PARTITION BY site_id, quality ORDER BY read_at_month, id) AS next2_read_at,
        ROUND(PERCENT_RANK() OVER w_quality_by_kwh, 3) AS kwh_percent_rank,
        SIZE(COLLECT_SET(quality) OVER w_read_at_year_site_id) AS distinct_quality_in_read_at_year_site_id,
        MAX(voltage_minus_kwh) OVER (PARTITION BY site_id, quality ORDER BY read_at_month, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS voltage_minus_kwh_max_2,
        ROW_NUMBER() OVER (PARTITION BY quality, site_tariff ORDER BY voltage_minus_kwh DESC, id) AS rn,
        kwh - LAG(kwh) OVER (PARTITION BY quality, site_id ORDER BY voltage_minus_kwh, id) AS kwh_change
    FROM with_details
    WINDOW w_site_id_quality_by_read_at_m AS (PARTITION BY site_id, quality ORDER BY read_at_month, id), w_read_at_year_site_id AS (PARTITION BY YEAR(read_at), site_id), w_quality_by_kwh AS (PARTITION BY quality ORDER BY kwh, id)
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        kwh,
        read_at_month,
        voltage_minus_kwh,
        site_tariff,
        rows_in_read_at_year_site_id,
        kwh_percent_rank,
        voltage_minus_kwh_max_2,
        kwh_change,
        MAX(voltage_minus_kwh) OVER (PARTITION BY quality) AS quality_max_voltage_minus_kwh,
        NTILE(4) OVER w_distinct_quality_in_read_at AS kwh_change_quartile,
        kwh_change - LAG(kwh_change) OVER (PARTITION BY site_tariff ORDER BY next2_read_at DESC, id) AS kwh_change_change
    FROM sequenced
    WINDOW w_distinct_quality_in_read_at AS (PARTITION BY distinct_quality_in_read_at_year_site_id ORDER BY kwh_change DESC, id)
),
stats AS (
    -- Summarise per group
    SELECT site_id, COUNT(*) AS n_rows, COUNT(DISTINCT id) AS distinct_ids
    FROM sequenced_2
    WHERE kwh_percent_rank > 0.2
    GROUP BY site_id
    HAVING COUNT(*) >= 2
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        site_id,
        n_rows,
        distinct_ids,
        RANK() OVER (PARTITION BY n_rows ORDER BY n_rows DESC) AS n_rows_rank,
        DENSE_RANK() OVER w_n_rows_by_n_rows_ties AS n_rows_dense_rank,
        ROUND(n_rows / NULLIF(SUM(n_rows) OVER (PARTITION BY n_rows), 0), 4) AS n_rows_share_of_n_rows
    FROM stats
    WINDOW w_n_rows_by_n_rows_ties AS (PARTITION BY n_rows ORDER BY n_rows DESC)
)
-- Final projection
SELECT
    site_id,
    n_rows,
    distinct_ids,
    n_rows_rank,
    n_rows_dense_rank,
    ROUND(n_rows_share_of_n_rows * 1.1, 2) AS n_rows_share_of_n_rows_adj
FROM with_windows
ORDER BY n_rows_share_of_n_rows_adj
