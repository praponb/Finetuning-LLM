WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_to,
        rate,
        CASE WHEN rate >= 5.9 THEN 'large' WHEN rate >= 4.2 THEN 'medium' ELSE 'small' END AS rate_tier
    FROM tariff_windows
    WHERE rate >= 3.0
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM valid
    WHERE rate < (SELECT MAX(rate) FROM valid)
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM screened
    WHERE rate < (SELECT MAX(rate) FROM screened)
),
per_group AS (
    -- Group-level totals
    SELECT
        YEAR(valid_to) AS valid_to_year,
        rate_tier,
        tariff,
        MIN(rate) AS min_rate,
        SUM(rate) FILTER (WHERE tariff IN ('FLAT', 'EV')) AS rate_tariff_in_flat_ev,
        SUM(rate) FILTER (WHERE tariff NOT IN ('EV', 'FLAT')) AS rate_tariff_not_in_ev_flat,
        COUNT(*) FILTER (WHERE tariff IN ('TOU', 'EV')) AS n_tariff_in_tou_ev,
        MAX(valid_to) AS last_valid_to,
        ARRAY_SORT(COLLECT_SET(tariff)) AS tariff_list
    FROM qualified
    GROUP BY YEAR(valid_to), rate_tier, tariff
    HAVING SUM(rate) > 3.7
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_group
    WHERE valid_to_year > (SELECT AVG(valid_to_year) FROM per_group)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(TRIM(tariff)) AS tariff_clean,
        CONCAT_WS(' / ', rate_tier, tariff) AS rate_tier_tariff_label
    FROM filtered
),
annotated AS (
    -- Add calculated columns
    SELECT
        valid_to_year,
        rate_tier,
        tariff,
        rate_tariff_in_flat_ev,
        n_tariff_in_tou_ev,
        last_valid_to,
        DATE_TRUNC('DAY', last_valid_to) AS last_valid_to_day,
        DATEDIFF(DATE '2026-01-01', last_valid_to) AS days_since_last_valid_to
    FROM enriched
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT last_valid_to, tariff, n_tariff_in_tou_ev
    FROM annotated
) src
PIVOT (
    MAX(n_tariff_in_tou_ev)
    FOR tariff IN ('EV', 'TOU', 'FLAT')
)
