WITH prepared AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM sites
    WHERE tariff <> 'EV'
),
stacked AS (
    -- Stack the branches
    SELECT
        id,
        tariff_or_unknown,
        commissioned_on,
        'site' AS origin
    FROM prepared
    WHERE zone IN ('Z3', 'Z2')
    UNION
    SELECT
        id,
        tariff,
        commissioned_on,
        'site_2' AS origin
    FROM sites
)
SELECT origin, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(commissioned_on) AS latest_commissioned_on
FROM stacked
GROUP BY origin
ORDER BY origin
