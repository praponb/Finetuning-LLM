WITH unioned AS (
    -- Stack the branches
    SELECT
        id AS ref_id,
        commissioned_on AS event_date,
        installed_kw,
        'site' AS origin
    FROM sites
    WHERE commissioned_on BETWEEN DATE '2024-06-27' AND DATE '2025-05-10'
    UNION ALL
    SELECT
        id,
        commissioned_on,
        installed_kw,
        'site_2' AS origin
    FROM sites
    WHERE installed_kw < 386.6
      AND site_name IS NOT NULL
)
SELECT origin, COUNT(*) AS n_rows, SUM(installed_kw) AS total_installed_kw, MAX(event_date) AS latest_event_date
FROM unioned
GROUP BY origin
ORDER BY origin
