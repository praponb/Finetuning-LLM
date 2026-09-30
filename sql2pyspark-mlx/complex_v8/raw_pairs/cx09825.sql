WITH covered AS (
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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        rate,
        energy_cost,
        ROUND(energy_cost / NULLIF(SUM(energy_cost) OVER (PARTITION BY MONTH(read_at), zone), 0), 4) AS energy_cost_share_of_read_at_month_zone,
        RANK() OVER w_zone_site_id_by_voltage_ties AS voltage_rank,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY MONTH(read_at), zone)) AS distinct_site_id_in_read_at_month_zone,
        SUM(kwh) OVER w_read_at_month_zone AS read_at_month_zone_sum_kwh,
        MAX(voltage) OVER w_read_at_month_zone AS read_at_month_zone_max_voltage
    FROM covered
    WINDOW w_zone_site_id_by_voltage_ties AS (PARTITION BY zone, site_id ORDER BY voltage ASC), w_read_at_month_zone AS (PARTITION BY MONTH(read_at), zone)
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) FILTER (WHERE quality = 'EST') AS reading_quality_count,
        MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        m.first_reading_read_at
    FROM with_trends s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
SELECT
    id,
    site_id,
    read_at,
    rate,
    energy_cost_share_of_read_at_month_zone,
    read_at_month_zone_sum_kwh,
    reading_quality_count,
    first_reading_read_at,
    ROUND((UNIX_TIMESTAMP(first_reading_read_at) - UNIX_TIMESTAMP(read_at)) / 3600, 1) AS hours_read_at_to_first_reading_read_at,
    COALESCE(voltage_rank, 0) AS voltage_rank_filled
FROM profiled
ORDER BY read_at_month_zone_sum_kwh, id
