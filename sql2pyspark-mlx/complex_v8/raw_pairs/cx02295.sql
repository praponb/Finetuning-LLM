WITH matched AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.voltage,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
totals AS (
    -- Group-level totals
    SELECT
        zone,
        site_id,
        PERCENTILE_APPROX(kwh, 0.5) AS median_kwh,
        SUM(rate) AS total_rate,
        MIN(read_at) AS first_read_at,
        COUNT(*) AS n_rows,
        ROUND(SUM(rate), 2) AS total_rate_2
    FROM matched
    GROUP BY zone, site_id
    HAVING COUNT(*) >= 2
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        zone,
        site_id,
        first_read_at,
        total_rate_2,
        LAST_VALUE(median_kwh) OVER (ORDER BY total_rate NULLS LAST, zone, site_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_median_kwh,
        ROUND(AVG(total_rate_2) OVER (PARTITION BY n_rows ORDER BY total_rate DESC, zone, site_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS total_rate_2_avg_6_prior,
        ROUND(total_rate_2 / NULLIF(SUM(total_rate_2) OVER (PARTITION BY n_rows, zone), 0), 4) AS total_rate_2_share_of_n_rows_zone,
        NTILE(10) OVER (PARTITION BY n_rows ORDER BY total_rate DESC, zone, site_id) AS total_rate_decile,
        LAG(median_kwh, 2) OVER (PARTITION BY site_id ORDER BY total_rate, zone, site_id) AS prev2_median_kwh
    FROM totals
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.zone,
        s.site_id,
        s.first_read_at,
        s.last_median_kwh,
        s.prev2_median_kwh,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage
    FROM ranked s
    JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT zone, site_id, metric, metric_value
FROM (
    SELECT zone, site_id, reading_voltage, reading_kwh, last_median_kwh
    FROM detail_rows
) src
UNPIVOT (
    metric_value FOR metric IN (reading_voltage, reading_kwh, last_median_kwh)
) unp
