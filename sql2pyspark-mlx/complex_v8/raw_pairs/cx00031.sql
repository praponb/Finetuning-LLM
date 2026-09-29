WITH base_sites AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        CASE WHEN installed_kw >= 386.6 THEN 'A' WHEN installed_kw >= 93.3 THEN 'B' ELSE 'C' END AS installed_kw_tier,
        SUBSTRING(TRIM(zone), 1, 2) AS zone_prefix,
        CONCAT_WS(' / ', site_name, zone) AS site_name_zone_label
    FROM sites
    WHERE tariff <> 'FLAT'
),
latest_per_key AS (
    -- Keep the earliest row per zone
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        installed_kw_tier,
        zone_prefix,
        site_name_zone_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone ORDER BY installed_kw, id) AS rn
        FROM base_sites
    ) ranked
    WHERE rn = 1
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM latest_per_key
    WHERE installed_kw > (SELECT AVG(installed_kw) FROM latest_per_key)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(zone, 'UNKNOWN') AS zone_or_unknown,
        COALESCE(installed_kw, 0.0) AS installed_kw_filled
    FROM without_matches
)
SELECT
    id,
    installed_kw_tier,
    zone_prefix,
    site_name_zone_label,
    zone_or_unknown,
    FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket,
    CASE WHEN installed_kw_filled >= 351.5 THEN 'large' WHEN installed_kw_filled >= 203.0 THEN 'medium' ELSE 'small' END AS installed_kw_filled_tier
FROM annotated
ORDER BY zone_prefix, id
LIMIT 20
