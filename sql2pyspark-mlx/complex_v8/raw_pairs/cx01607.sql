WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.voltage) AS prior_voltage_3d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(prior_rows_3d) OVER w_prior_rows_3d_site_id AS prior_rows_3d_site_id_sum_prior_rows_3d,
        ROUND(PERCENT_RANK() OVER (PARTITION BY prior_rows_3d ORDER BY read_at ASC), 3) AS read_at_percent_rank,
        LEAD(prior_rows_3d) OVER (PARTITION BY prior_rows_3d ORDER BY read_at ASC, id) AS next_prior_rows_3d,
        ROUND(AVG(prior_voltage_3d) OVER (PARTITION BY prior_rows_3d, site_id), 2) AS prior_rows_3d_site_id_avg_prior_voltage_3d,
        ROUND(AVG(prior_rows_3d) OVER (PARTITION BY prior_rows_3d ORDER BY read_at ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS prior_rows_3d_avg_4,
        ROUND(AVG(prior_voltage_3d) OVER (PARTITION BY prior_rows_3d ORDER BY read_at ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS prior_voltage_3d_avg_2_prior
    FROM in_window
    WINDOW w_prior_rows_3d_site_id AS (PARTITION BY prior_rows_3d, site_id)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(read_at_percent_rank * 0.9, 2) AS read_at_percent_rank_adj,
        FLOOR(prior_voltage_3d / 250) * 250 AS prior_voltage_3d_bucket,
        QUARTER(read_at) AS read_at_quarter
    FROM with_trends
    WHERE next_prior_rows_3d < 5
)
SELECT
    id,
    read_at,
    prior_rows_3d,
    prior_rows_3d_site_id_sum_prior_rows_3d,
    read_at_percent_rank,
    next_prior_rows_3d,
    prior_rows_3d_avg_4,
    read_at_percent_rank_adj,
    prior_voltage_3d_bucket,
    read_at_quarter
FROM scored
ORDER BY prior_voltage_3d_bucket DESC, read_at_quarter DESC, id
LIMIT 10
