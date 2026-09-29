WITH all_events AS (
    -- Stack the branches
    SELECT tariff, commissioned_on AS event_date
    FROM sites
    WHERE installed_kw <= 373.3
      AND LENGTH(TRIM(site_name)) >= 7
    UNION ALL
    SELECT zone AS tariff, commissioned_on AS event_date
    FROM sites
    WHERE installed_kw < 386.6
      AND tariff NOT IN ('TOU', 'EV')
    UNION ALL
    SELECT site_name AS tariff, commissioned_on AS event_date
    FROM sites
    WHERE tariff IN ('EV', 'FLAT')
)
SELECT tariff, COUNT(*) AS n_rows, MAX(event_date) AS latest_event_date
FROM all_events
GROUP BY tariff
ORDER BY tariff
