WITH cleaned AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        commissioned_on,
        LENGTH(TRIM(zone)) AS zone_len,
        UPPER(TRIM(zone)) AS zone_clean,
        DATE_FORMAT(commissioned_on, 'MMM yyyy') AS commissioned_on_label
    FROM sites
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM cleaned
    WHERE zone_len > (SELECT MIN(zone_len) FROM cleaned)
)
-- Final projection
SELECT
    id,
    zone,
    tariff,
    commissioned_on,
    zone_len
FROM without_matches
