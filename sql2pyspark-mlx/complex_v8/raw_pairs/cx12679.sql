WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.voltage) AS prior_voltage_12h
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.site_id, a.read_at
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY prior_rows_12h) AS rows_in_prior_rows_12h,
        NTH_VALUE(prior_voltage_12h, 2) OVER (ORDER BY prior_rows_12h, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prior_voltage_12h,
        ROUND(AVG(prior_voltage_12h) OVER (ORDER BY prior_rows_12h, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS prior_voltage_12h_avg_7
    FROM covered
)
SELECT
    id,
    site_id,
    prior_rows_12h,
    prior_voltage_12h,
    rows_in_prior_rows_12h,
    second_prior_voltage_12h,
    prior_voltage_12h_avg_7
FROM with_trends
WHERE rows_in_prior_rows_12h <= 11
ORDER BY second_prior_voltage_12h, rows_in_prior_rows_12h DESC
