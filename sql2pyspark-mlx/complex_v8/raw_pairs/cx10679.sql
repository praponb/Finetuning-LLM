WITH combined AS (
    -- Stack the branches
    SELECT
        'site' AS source,
        id,
        installed_kw,
        commissioned_on AS event_date
    FROM sites
    WHERE zone = 'Z4'
    UNION ALL
    SELECT
        'site_2' AS source,
        id,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE commissioned_on IS NOT NULL
)
SELECT source, COUNT(*) AS n_rows, SUM(installed_kw) AS total_installed_kw, MAX(event_date) AS latest_event_date
FROM combined
GROUP BY source
ORDER BY source
