WITH in_window AS (
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
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        zone,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id, zone), 2) AS site_id_zone_avg_kwh,
        LEAD(kwh, 3, 0) OVER w_all_by_read_at AS next3_kwh,
        ROUND(rate - AVG(rate) OVER w_site_id_zone, 2) AS rate_vs_site_id_zone_avg
    FROM in_window
    WINDOW w_all_by_read_at AS (ORDER BY read_at, id), w_site_id_zone AS (PARTITION BY site_id, zone)
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_trends s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.read_at >= TIMESTAMP '2025-01-19 07:00:00'
        )
      AND s.rate_vs_site_id_zone_avg BETWEEN -5.6 AND 5.6
)
SELECT id, metric, metric_value
FROM (
    SELECT id, voltage, kwh
    FROM with_activity
) src
UNPIVOT (
    metric_value FOR metric IN (voltage, kwh)
) unp
