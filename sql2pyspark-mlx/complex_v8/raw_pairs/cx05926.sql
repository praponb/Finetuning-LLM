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
deduped AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        prior_rows_12h,
        prior_voltage_12h
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY prior_voltage_12h DESC, id DESC) AS recency_rank
        FROM covered
    ) ranked
    WHERE recency_rank = 1
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.prior_rows_12h,
        s.prior_voltage_12h,
        si.tariff AS site_tariff,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM deduped s
    LEFT JOIN sites si ON si.id = s.site_id
),
summary AS (
    -- Group-level totals
    SELECT
        YEAR(read_at) AS read_at_year,
        ROUND(AVG(prior_rows_12h), 2) AS avg_prior_rows_12h,
        COUNT(*) AS n_rows
    FROM joined
    GROUP BY YEAR(read_at)
)
SELECT
    read_at_year,
    avg_prior_rows_12h,
    n_rows,
    GREATEST(read_at_year, n_rows) AS greatest_read_at_year_n_rows
FROM summary
ORDER BY greatest_read_at_year_n_rows DESC
