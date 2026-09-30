WITH filtered AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        ABS(installed_kw - 284.0) AS installed_kw_distance,
        UPPER(TRIM(site_name)) AS site_name_clean,
        CONCAT(zone, '#', CAST(id AS STRING)) AS zone_ref
    FROM sites
    WHERE commissioned_on IS NOT NULL
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at
    FROM filtered s
    LEFT JOIN reading_stats m ON m.site_id = s.id
),
unique_rows AS (
    -- Keep the latest row per tariff, zone
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        installed_kw_distance,
        site_name_clean,
        zone_ref,
        last_reading_read_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff, zone ORDER BY installed_kw DESC, id DESC) AS recency_rank
        FROM with_counts
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    commissioned_on,
    site_name_clean,
    zone_ref,
    last_reading_read_at
FROM unique_rows
ORDER BY id DESC, site_name_clean DESC
