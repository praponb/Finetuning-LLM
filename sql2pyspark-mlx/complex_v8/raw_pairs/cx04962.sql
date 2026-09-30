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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
enriched AS (
    -- Attach site attributes
    SELECT s.*, si.zone AS site_zone, si.site_name
    FROM covered s
    LEFT JOIN sites si ON si.id = s.site_id
),
enriched_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.rate,
        s.energy_cost,
        s.site_zone,
        s.site_name,
        si.tariff AS site_tariff,
        si.site_name AS site_site_name,
        si.commissioned_on AS site_commissioned_on
    FROM enriched s
    JOIN sites si ON si.id = s.site_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(energy_cost / NULLIF(SUM(energy_cost) OVER w_site_tariff, 0), 4) AS energy_cost_share_of_site_tariff,
        MAX(rate) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), site_tariff ORDER BY energy_cost, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rate_max_2_prior
    FROM enriched_2
    WINDOW w_site_tariff AS (PARTITION BY site_tariff)
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    rate,
    energy_cost,
    site_zone,
    site_tariff,
    site_site_name,
    site_commissioned_on,
    energy_cost_share_of_site_tariff,
    rate_max_2_prior,
    ABS(rate_max_2_prior - 16.3) AS rate_max_2_prior_distance
FROM ranked
ORDER BY rate_max_2_prior
