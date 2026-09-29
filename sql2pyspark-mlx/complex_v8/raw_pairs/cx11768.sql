WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        ADD_MONTHS(commissioned_on, 6) AS commissioned_on_plus_6m,
        DATE_TRUNC('WEEK', commissioned_on) AS commissioned_on_week
    FROM sites
    WHERE YEAR(commissioned_on) = 2024
      AND tariff NOT IN ('FLAT', 'EV')
),
grouped AS (
    -- Group-level totals
    SELECT
        tariff_or_unknown,
        COUNT(*) FILTER (WHERE tariff = 'TOU') AS n_tariff_tou,
        COUNT(*) AS cnt
    FROM valid
    GROUP BY 1
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        tariff_or_unknown,
        n_tariff_tou,
        cnt,
        ROUND(cnt / NULLIF(SUM(cnt) OVER (PARTITION BY tariff_or_unknown), 0), 4) AS cnt_share_of_tariff_or_unknown,
        ROUND(CUME_DIST() OVER (PARTITION BY n_tariff_tou ORDER BY cnt DESC, tariff_or_unknown), 3) AS cnt_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY tariff_or_unknown ORDER BY n_tariff_tou DESC, tariff_or_unknown) AS seq_no
    FROM grouped
)
-- Final projection
SELECT
    tariff_or_unknown,
    n_tariff_tou,
    cnt,
    cnt_share_of_tariff_or_unknown,
    cnt_cume_dist
FROM with_trends
