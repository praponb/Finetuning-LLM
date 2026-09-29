WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.voltage) AS prior_voltage_1d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
deduped AS (
    -- Keep the earliest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        prior_rows_1d,
        prior_voltage_1d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY read_at, id) AS recency_rank
        FROM in_window
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with CUBE
SELECT
    site_id,
    prior_rows_1d,
    SUM(prior_voltage_1d) AS total_prior_voltage_1d,
    COUNT(DISTINCT id) AS distinct_ids
FROM deduped
GROUP BY CUBE(site_id, prior_rows_1d)
ORDER BY site_id NULLS LAST, prior_rows_1d NULLS LAST
