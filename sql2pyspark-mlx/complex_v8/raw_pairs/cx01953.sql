WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        CASE WHEN voltage >= 229.9 THEN 'HIGH' WHEN voltage >= 219.5 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_tier
    FROM meter_readings
    WHERE (read_at >= TIMESTAMP '2025-02-06 08:00:00' OR kwh BETWEEN 26.9 AND 109.0)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        DATE_FORMAT(read_at_month, 'yyyy-MM-dd') AS read_at_month_label
    FROM prepared_readings
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.read_at_month,
        s.voltage_tier,
        s.read_at_month_label,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM tagged s
    LEFT JOIN sites si ON si.id = s.site_id
),
latest_per_key AS (
    -- Keep the earliest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        read_at_month,
        voltage_tier,
        read_at_month_label,
        site_installed_kw,
        site_name
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY read_at_month, id) AS rn
        FROM joined
    ) ranked
    WHERE rn = 1
)
SELECT id, measure, value
FROM (
    SELECT id, site_installed_kw, kwh
    FROM latest_per_key
) src
UNPIVOT (
    value FOR measure IN (site_installed_kw AS site_installed_kw_v, kwh AS kwh_v)
) unp
