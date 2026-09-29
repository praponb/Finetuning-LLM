WITH valid AS (
    -- Scope the meter_readings rows
    SELECT *
    FROM meter_readings
    WHERE voltage >= 218.2
),
deduped AS (
    -- Keep the latest row per quality, site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY read_at DESC, id DESC) AS recency_rank
        FROM valid
    ) ranked
    WHERE recency_rank = 1
),
enriched AS (
    -- Attach site attributes
    SELECT
        s.*,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM deduped s
    JOIN sites si ON si.id = s.site_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, reading
FROM (
    SELECT id, kwh, site_installed_kw, voltage
    FROM enriched
) src
UNPIVOT (
    reading FOR metric IN (kwh AS m_kwh, site_installed_kw AS site_installed_kw_v, voltage AS m_voltage)
) unp
ORDER BY id, metric
