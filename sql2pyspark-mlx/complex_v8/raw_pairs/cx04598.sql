WITH scoped AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug
    FROM tariff_windows
    WHERE rate > 3.0
),
summary AS (
    -- Group-level totals
    SELECT
        TO_DATE(valid_from) AS valid_from_day,
        tariff,
        DATE_TRUNC('MONTH', valid_to) AS valid_to_month,
        ROUND(100 * COUNT(CASE WHEN tariff = 'EV' THEN 1 END) / COUNT(*), 1) AS pct_tariff_ev,
        MAX(valid_from) AS last_valid_from,
        COUNT(*) FILTER (WHERE tariff NOT IN ('TOU', 'EV')) AS n_tariff_not_in_tou_ev,
        ROUND(100 * COUNT(CASE WHEN tariff = 'TOU' THEN 1 END) / COUNT(*), 1) AS pct_tariff_tou
    FROM scoped
    GROUP BY 1, 2, 3
),
scored AS (
    -- Label and score rows
    SELECT *, DATE_TRUNC('MONTH', valid_from_day) AS valid_from_day_month
    FROM summary
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM scored
    WHERE pct_tariff_ev > (SELECT AVG(pct_tariff_ev) FROM scored)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY n_tariff_not_in_tou_ev ORDER BY valid_from_day_month ASC, valid_from_day, tariff, valid_to_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(10) OVER (PARTITION BY n_tariff_not_in_tou_ev ORDER BY valid_from_day_month ASC, valid_from_day, tariff, valid_to_month) AS valid_from_day_month_decile,
        NTILE(3) OVER w_n_tariff_not_in_tou_ev_by_va_2 AS valid_from_day_month_tercile,
        ROUND(pct_tariff_ev - AVG(pct_tariff_ev) OVER (PARTITION BY YEAR(valid_to_month)), 2) AS pct_tariff_ev_vs_valid_to_month_year_avg
    FROM without_matches
    WINDOW w_n_tariff_not_in_tou_ev_by_va_2 AS (PARTITION BY n_tariff_not_in_tou_ev ORDER BY valid_from_day_month ASC, valid_from_day, tariff, valid_to_month)
),
enriched AS (
    -- Add calculated columns
    SELECT
        valid_from_day,
        tariff,
        valid_to_month,
        n_tariff_not_in_tou_ev,
        pct_tariff_tou,
        valid_from_day_month,
        running_count,
        valid_from_day_month_tercile,
        pct_tariff_ev_vs_valid_to_month_year_avg,
        LAST_DAY(valid_from_day) AS valid_from_day_month_end
    FROM sequenced
)
SELECT *
FROM (
    SELECT valid_from_day, valid_from_day_month_tercile, tariff, running_count
    FROM enriched
) src
PIVOT (
    COUNT(running_count)
    FOR tariff IN ('EV', 'FLAT', 'TOU')
)
ORDER BY valid_from_day, valid_from_day_month_tercile
