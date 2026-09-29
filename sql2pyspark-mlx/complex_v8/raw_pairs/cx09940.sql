WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh
    FROM meter_readings
    WHERE voltage <= 232.7
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        quality,
        voltage_per_kwh,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label
    FROM filtered
    WHERE kwh < 81.3
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.read_at_label,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM annotated s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff IS NOT NULL
),
joined_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.read_at_label,
        s.site_commissioned_on,
        si.zone AS site_zone
    FROM with_details s
    JOIN sites si ON si.id = s.site_id
),
annotated_2 AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(' / ', read_at_label, site_zone) AS read_at_label_site_zone_label
    FROM joined_site
),
latest AS (
    -- Keep the earliest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        read_at_label,
        site_commissioned_on,
        site_zone,
        read_at_label_site_zone_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY site_commissioned_on, id) AS recency_rank
        FROM annotated_2
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    site_id,
    read_at,
    site_zone,
    read_at_label_site_zone_label,
    CASE WHEN site_zone IN ('Z3', 'Z4') THEN 'group_a' WHEN site_zone = 'Z2' THEN 'group_b' ELSE 'other' END AS site_zone_group,
    CONCAT_WS('|', read_at_label, site_zone) AS read_at_label_site_zone_label_2
FROM latest
ORDER BY site_id, id
LIMIT 20
