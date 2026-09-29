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
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(rate) OVER (ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_rate,
        ROUND(rate - AVG(rate) OVER (PARTITION BY MONTH(read_at), quality), 2) AS rate_vs_read_at_month_quality_avg,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY MONTH(read_at), quality)) AS distinct_site_id_in_read_at_month_quality,
        energy_cost - LAG(energy_cost) OVER w_site_id_by_kwh AS energy_cost_change,
        ROW_NUMBER() OVER w_all_by_read_at AS rn
    FROM matched
    WINDOW w_site_id_by_kwh AS (PARTITION BY site_id ORDER BY kwh DESC, id), w_all_by_read_at AS (ORDER BY read_at ASC, id)
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    zone,
    rate,
    energy_cost,
    first_rate,
    energy_cost_change,
    rn,
    LOWER(TRIM(zone)) AS zone_clean,
    INITCAP(TRIM(quality)) AS quality_clean
FROM with_windows
WHERE site_id IS NOT NULL
ORDER BY energy_cost DESC NULLS FIRST, rate
