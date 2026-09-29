WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATE_TRUNC('QUARTER', read_at) AS read_at_quarter,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-19 09:00:00'
      AND kwh > 43.1
),
site_id_stats AS (
    -- Median kwh per site_id
    SELECT site_id, PERCENTILE_APPROX(kwh, 0.5) AS median_kwh
    FROM prepared
    GROUP BY site_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_kwh, ROUND(s.kwh - g.median_kwh, 2) AS kwh_minus_median
    FROM prepared s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.kwh >= g.median_kwh
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY MONTH(read_at_quarter) ORDER BY read_at_quarter DESC, id) AS read_at_quarter_dense_rank,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id) AS rows_in_read_at_month_site_id,
        LEAD(kwh_minus_median, 2) OVER w_read_at_quarter_month_by_rea AS next2_kwh_minus_median
    FROM compared
    WINDOW w_read_at_quarter_month_by_rea AS (PARTITION BY MONTH(read_at_quarter) ORDER BY read_at_quarter DESC, id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(days_since_read_at) OVER w_read_at_quarter_dense_rank AS read_at_quarter_dense_rank_sum_days_since_read_a,
        ROUND(voltage - AVG(voltage) OVER w_read_at_quarter_dense_rank, 2) AS voltage_vs_read_at_quarter_dense_rank_avg,
        voltage - LAG(voltage) OVER (ORDER BY median_kwh ASC, id) AS voltage_change,
        MIN(voltage) OVER (PARTITION BY read_at_quarter_dense_rank) AS read_at_quarter_dense_rank_min_voltage,
        NTILE(10) OVER (ORDER BY next2_kwh_minus_median, id) AS next2_kwh_minus_median_decile,
        SUM(median_kwh) OVER (ORDER BY median_kwh ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS median_kwh_sum_2_prior
    FROM ranked
    WINDOW w_read_at_quarter_dense_rank AS (PARTITION BY read_at_quarter_dense_rank)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, rows_in_read_at_month_site_id, next2_kwh_minus_median_decile, days_since_read_at, read_at_quarter_dense_rank_sum_days_since_read_a
    FROM metrics
) src
UNPIVOT (
    reading FOR measure IN (rows_in_read_at_month_site_id, next2_kwh_minus_median_decile, days_since_read_at, read_at_quarter_dense_rank_sum_days_since_read_a)
) unp
ORDER BY id, measure
