WITH filtered AS (
    -- Scope the sites rows
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket
    FROM sites
    WHERE zone IN ('Z2', 'Z3')
      AND tariff NOT IN ('FLAT', 'TOU')
),
stacked AS (
    -- Stack the branches
    SELECT
        installed_kw_bucket,
        commissioned_on,
        zone,
        'site' AS src
    FROM filtered
    UNION ALL
    SELECT
        id,
        commissioned_on,
        zone,
        'site_2' AS src
    FROM sites
)
SELECT src, COUNT(*) AS n_rows, SUM(installed_kw_bucket) AS total_installed_kw_bucket, MAX(commissioned_on) AS latest_commissioned_on
FROM stacked
GROUP BY src
ORDER BY src
