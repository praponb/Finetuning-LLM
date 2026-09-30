WITH covered AS (
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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        quality,
        zone,
        rate,
        energy_cost,
        NTH_VALUE(energy_cost, 2) OVER (ORDER BY rate, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_energy_cost,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        ROUND(rate / NULLIF(SUM(rate) OVER (PARTITION BY site_id), 0), 4) AS rate_share_of_site_id,
        LAG(kwh) OVER (ORDER BY rate, id) AS prev_kwh
    FROM covered
)
SELECT
    id,
    zone,
    rate,
    energy_cost,
    second_energy_cost,
    prev_kwh
FROM windowed
ORDER BY prev_kwh DESC
