WITH valid AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        tariff,
        commissioned_on,
        LENGTH(TRIM(tariff)) AS tariff_len
    FROM sites
    WHERE zone IN ('Z3', 'Z2')
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM valid
    WHERE tariff_len > (SELECT AVG(tariff_len) FROM valid)
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM without_matches
    WHERE tariff_len < (SELECT MAX(tariff_len) FROM without_matches)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(tariff_len, 0) AS tariff_len_filled,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM qualified
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, tariff_len, tariff_len_filled
    FROM labeled
) src
UNPIVOT (
    metric_value FOR field IN (tariff_len, tariff_len_filled)
) unp
ORDER BY id, field
