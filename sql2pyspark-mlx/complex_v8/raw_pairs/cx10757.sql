WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND voltage BETWEEN 220.7 AND 238.3
),
with_parent AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on
    FROM cleaned_readings s
    LEFT JOIN sites si ON si.id = s.site_id AND si.zone <> 'Z2'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id, site_tariff), 0), 4) AS voltage_share_of_site_id_site_tariff,
        MAX(kwh) OVER (PARTITION BY site_tariff, site_id) AS site_tariff_site_id_max_kwh,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY site_tariff ORDER BY site_commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        ROUND(voltage - AVG(voltage) OVER w_site_tariff_site_id, 2) AS voltage_vs_site_tariff_site_id_avg,
        RANK() OVER (PARTITION BY site_tariff ORDER BY site_commissioned_on DESC, id) AS site_commissioned_on_rank,
        ROUND(AVG(voltage) OVER (PARTITION BY site_tariff ORDER BY site_commissioned_on DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_1_prior,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_tariff, site_id), 2) AS kwh_vs_site_tariff_site_id_avg
    FROM with_parent
    WINDOW w_site_tariff_site_id AS (PARTITION BY site_tariff, site_id)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        site_tariff,
        site_commissioned_on,
        voltage_share_of_site_id_site_tariff,
        site_tariff_site_id_max_kwh,
        first_known_kwh,
        voltage_vs_site_tariff_site_id_avg,
        voltage_avg_1_prior,
        kwh_vs_site_tariff_site_id_avg
    FROM sequenced
    WHERE site_commissioned_on_rank <= 5
      AND site_commissioned_on >= DATE '2024-09-15'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        kwh,
        voltage,
        site_tariff,
        site_commissioned_on,
        voltage_share_of_site_id_site_tariff,
        first_known_kwh,
        voltage_vs_site_tariff_site_id_avg,
        voltage_avg_1_prior,
        kwh_vs_site_tariff_site_id_avg,
        LAST_VALUE(site_commissioned_on) IGNORE NULLS OVER (ORDER BY voltage_vs_site_tariff_site_id_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_site_commissioned_on,
        ROUND(AVG(site_tariff_site_id_max_kwh) OVER (PARTITION BY site_tariff, site_id), 2) AS site_tariff_site_id_avg_site_tariff_site_id_max_,
        RANK() OVER w_site_tariff_site_id_by_site AS site_tariff_site_id_max_kwh_rank,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY site_tariff, site_id ORDER BY site_tariff_site_id_max_kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh
    FROM top_rows
    WINDOW w_site_tariff_site_id_by_site AS (PARTITION BY site_tariff, site_id ORDER BY site_tariff_site_id_max_kwh)
)
-- Final projection
SELECT
    id,
    kwh,
    voltage,
    site_tariff,
    site_commissioned_on,
    voltage_share_of_site_id_site_tariff,
    first_known_kwh,
    voltage_vs_site_tariff_site_id_avg,
    kwh_vs_site_tariff_site_id_avg,
    last_known_site_commissioned_on,
    site_tariff_site_id_avg_site_tariff_site_id_max_,
    site_tariff_site_id_max_kwh_rank,
    second_kwh
FROM windowed
ORDER BY voltage NULLS LAST, voltage_share_of_site_id_site_tariff
