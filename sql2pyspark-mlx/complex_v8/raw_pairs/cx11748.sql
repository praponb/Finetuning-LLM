WITH base AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        CONCAT_WS('|', tariff, site_name) AS tariff_site_name_label
    FROM sites
    WHERE commissioned_on >= DATE '2024-06-27'
),
all_events AS (
    -- Stack the branches
    SELECT id AS key_id, commissioned_on, installed_kw
    FROM base
    WHERE installed_kw >= 141.6
      AND tariff_site_name_label IS NOT NULL
    UNION ALL
    SELECT id, commissioned_on, installed_kw
    FROM sites
    UNION ALL
    SELECT id, commissioned_on, installed_kw
    FROM sites
    WHERE commissioned_on >= DATE '2024-09-15'
)
SELECT key_id, COUNT(*) AS n_rows, SUM(installed_kw) AS total_installed_kw, MAX(commissioned_on) AS latest_commissioned_on
FROM all_events
GROUP BY key_id
ORDER BY key_id
