WITH joined_ranges AS (
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
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at)), 0), 4) AS voltage_share_of_read_at_month,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at)), 2) AS kwh_vs_read_at_month_avg,
        LAST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY rate DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at)) AS rows_in_read_at_month
    FROM joined_ranges
    WINDOW w_read_at_month AS (PARTITION BY DATE_TRUNC('MONTH', read_at))
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
      AND s.rate > 2.2
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id), 2) AS site_id_avg_kwh,
        SIZE(COLLECT_SET(zone) OVER (PARTITION BY site_id)) AS distinct_zone_in_site_id
    FROM qualified
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    rate,
    voltage_share_of_read_at_month,
    kwh_vs_read_at_month_avg,
    last_known_read_at,
    rows_in_read_at_month,
    site_id_avg_kwh,
    distinct_zone_in_site_id,
    CONCAT(zone, '#', CAST(id AS STRING)) AS zone_ref
FROM with_trends
ORDER BY site_id_avg_kwh
