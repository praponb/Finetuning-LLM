WITH matched AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.quality,
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
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        zone,
        rate,
        ROUND(AVG(energy_cost) OVER (PARTITION BY quality), 2) AS quality_avg_energy_cost,
        MAX(kwh) OVER (PARTITION BY quality, zone ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far,
        RANK() OVER (PARTITION BY quality, zone ORDER BY read_at) AS read_at_rank,
        COUNT(*) OVER w_quality AS rows_in_quality
    FROM matched
    WINDOW w_quality AS (PARTITION BY quality)
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality = 'OK'
        )
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    zone,
    quality_avg_energy_cost,
    read_at_rank,
    rows_in_quality
FROM with_activity
ORDER BY id, zone NULLS LAST
