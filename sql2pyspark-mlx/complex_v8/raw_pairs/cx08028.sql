WITH scoped_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        ADD_MONTHS(commissioned_on, 6) AS commissioned_on_plus_6m
    FROM sites
    WHERE commissioned_on >= DATE '2024-10-28'
),
stacked AS (
    -- Stack the branches
    SELECT id AS ref_id, commissioned_on_plus_6m AS day
    FROM scoped_sites
    WHERE commissioned_on_plus_6m >= DATE '2024-12-01'
      AND zone IS NOT NULL
    UNION ALL
    SELECT id, commissioned_on
    FROM sites
    WHERE commissioned_on < DATE '2025-05-07'
      AND zone <> 'Z3'
    UNION ALL
    SELECT id, commissioned_on
    FROM sites
    WHERE tariff NOT IN ('TOU', 'FLAT')
      AND zone <> 'Z4'
)
SELECT ref_id, COUNT(*) AS n_rows, MAX(day) AS latest_day
FROM stacked
GROUP BY ref_id
ORDER BY ref_id
