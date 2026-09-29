WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        YEAR(read_at) AS read_at_year
    FROM meter_readings
    WHERE quality = 'OK'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        read_at_year,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY read_at_year ORDER BY read_at_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        SUM(read_at_year) OVER (PARTITION BY read_at_year ORDER BY read_at_year ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS read_at_year_sum_2_prior
    FROM cleaned_readings
),
labeled AS (
    -- Label and score rows
    SELECT *, QUARTER(read_at) AS read_at_quarter
    FROM ranked
),
site_id_stats AS (
    -- Median read_at_quarter per site_id
    SELECT site_id, PERCENTILE_APPROX(read_at_quarter, 0.5) AS median_read_at_quarter
    FROM labeled
    GROUP BY site_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_read_at_quarter,
        ROUND(s.read_at_quarter - g.median_read_at_quarter, 2) AS read_at_quarter_minus_median
    FROM labeled s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.read_at_quarter >= g.median_read_at_quarter
)
-- Subtotals with ROLLUP
SELECT
    read_at_quarter,
    site_id,
    MIN(read_at) AS first_read_at,
    ROUND(SUM(second_kwh) / NULLIF(SUM(read_at_year), 0), 3) AS second_kwh_to_read_at_year_ratio,
    MAX(voltage) AS max_voltage
FROM compared
GROUP BY ROLLUP(read_at_quarter, site_id)
ORDER BY read_at_quarter NULLS LAST, site_id NULLS LAST
