WITH joined_ranges AS (
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
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(prior_rows_3d, 2) OVER w_read_at_month_site_id_by_rea AS prev2_prior_rows_3d,
        COUNT(*) OVER (PARTITION BY prior_rows_3d) AS rows_in_prior_rows_3d,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(read_at), site_id ORDER BY read_at ASC), 3) AS read_at_percent_rank,
        ROUND(prior_rows_3d - AVG(prior_rows_3d) OVER (PARTITION BY prior_rows_3d), 2) AS prior_rows_3d_vs_prior_rows_3d_avg
    FROM joined_ranges
    WINDOW w_read_at_month_site_id_by_rea AS (PARTITION BY MONTH(read_at), site_id ORDER BY read_at ASC, id)
)
SELECT id, metric, amount
FROM (
    SELECT id, prev2_prior_rows_3d, rows_in_prior_rows_3d, prior_rows_3d
    FROM with_windows
) src
UNPIVOT (
    amount FOR metric IN (prev2_prior_rows_3d AS prev2_prior_rows_3d_v, rows_in_prior_rows_3d AS rows_in_prior_rows_3d_v, prior_rows_3d AS m_prior_rows_3d)
) unp
ORDER BY id, metric
