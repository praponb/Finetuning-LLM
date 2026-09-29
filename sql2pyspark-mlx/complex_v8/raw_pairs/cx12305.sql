WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        DATEDIFF(DATE '2025-06-30', commissioned_on) AS days_since_commissioned_on,
        QUARTER(commissioned_on) AS commissioned_on_quarter,
        DATEDIFF(DATE '2025-06-30', commissioned_on) AS days_since_commissioned_on_2
    FROM sites
    WHERE tariff = 'FLAT'
      AND UPPER(TRIM(site_name)) LIKE 'S%'
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM scoped
    WHERE commissioned_on_quarter > (SELECT AVG(commissioned_on_quarter) FROM scoped)
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        site_name,
        zone,
        commissioned_on,
        days_since_commissioned_on,
        GREATEST(commissioned_on_quarter, installed_kw) AS greatest_commissioned_on_quarter_installed_kw,
        SUBSTRING(TRIM(site_name), 1, 2) AS site_name_prefix
    FROM screened
)
-- Final projection
SELECT
    id,
    site_name,
    zone,
    commissioned_on,
    days_since_commissioned_on,
    site_name_prefix,
    TRUNC(commissioned_on, 'MM') AS commissioned_on_month_start,
    COALESCE(zone, 'UNKNOWN') AS zone_or_unknown
FROM tagged
ORDER BY site_name_prefix, commissioned_on DESC
