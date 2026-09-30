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
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(kwh) OVER (PARTITION BY zone, site_id), 2) AS zone_site_id_avg_kwh,
        NTH_VALUE(kwh, 2) OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        RANK() OVER (ORDER BY kwh DESC, id) AS kwh_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY voltage NULLS LAST), 3) AS voltage_percent_rank,
        ROUND(rate / NULLIF(SUM(rate) OVER (PARTITION BY zone, site_id), 0), 4) AS rate_share_of_zone_site_id
    FROM joined_ranges
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, amount
FROM (
    SELECT id, rate, voltage
    FROM sequenced
) src
UNPIVOT (
    amount FOR metric_name IN (rate AS m_rate, voltage AS voltage_v)
) unp
