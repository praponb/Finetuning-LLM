WITH unioned AS (
    -- Stack the branches
    SELECT
        id AS key_id,
        commissioned_on,
        installed_kw,
        'site' AS origin
    FROM sites
    UNION ALL
    SELECT
        id AS key_id,
        commissioned_on,
        installed_kw,
        'site_2' AS origin
    FROM sites
    UNION ALL
    SELECT
        id AS key_id,
        commissioned_on,
        installed_kw,
        'site_3' AS origin
    FROM sites
)
SELECT origin, COUNT(*) AS n_rows, SUM(installed_kw) AS total_installed_kw, MAX(commissioned_on) AS latest_commissioned_on
FROM unioned
GROUP BY origin
ORDER BY origin
