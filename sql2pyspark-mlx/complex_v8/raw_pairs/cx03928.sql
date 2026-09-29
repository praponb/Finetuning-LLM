WITH scoped_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        tariff,
        installed_kw,
        commissioned_on,
        DATE_FORMAT(commissioned_on, 'MMM yyyy') AS commissioned_on_label,
        DATE_FORMAT(commissioned_on, 'yyyy-MM') AS commissioned_on_label_2
    FROM sites
    WHERE tariff IS NOT NULL
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', commissioned_on_label_2, tariff) AS commissioned_on_label_2_tariff_label,
        UPPER(TRIM(commissioned_on_label)) AS commissioned_on_label_clean,
        LOWER(REGEXP_REPLACE(TRIM(commissioned_on_label_2), ' ', '_')) AS commissioned_on_label_2_slug
    FROM scoped_sites
)
-- Final projection
SELECT
    id,
    installed_kw,
    commissioned_on,
    commissioned_on_label_2,
    commissioned_on_label_2_tariff_label,
    commissioned_on_label_clean,
    SUBSTRING(TRIM(commissioned_on_label), 1, 4) AS commissioned_on_label_prefix,
    ROUND(installed_kw * 1.07, 2) AS installed_kw_adj
FROM derived
ORDER BY commissioned_on_label_prefix, installed_kw_adj DESC NULLS FIRST, id
LIMIT 20
