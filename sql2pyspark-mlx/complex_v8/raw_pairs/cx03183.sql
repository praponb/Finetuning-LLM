WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM sites
    WHERE commissioned_on IS NOT NULL
),
latest AS (
    -- Keep the latest row per zone
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone ORDER BY commissioned_on DESC, id DESC) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY tariff, zone ORDER BY commissioned_on DESC, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY installed_kw, id) AS seq_no_2,
        ROUND(AVG(installed_kw) OVER (PARTITION BY tariff, zone ORDER BY commissioned_on DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS installed_kw_avg_2,
        COUNT(*) OVER (PARTITION BY tariff, zone) AS rows_in_tariff_zone,
        MIN(installed_kw) OVER (ORDER BY installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_installed_kw_so_far
    FROM latest
),
zone_stats AS (
    -- Avg seq_no per zone
    SELECT zone, AVG(seq_no) AS avg_seq_no
    FROM with_windows
    GROUP BY zone
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_seq_no, ROUND(s.seq_no - g.avg_seq_no, 2) AS seq_no_minus_avg
    FROM with_windows s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.seq_no > g.avg_seq_no
)
SELECT
    seq_no_2,
    seq_no,
    tariff,
    GROUPING(seq_no_2) AS is_total_seq_no_2,
    COUNT(*) AS row_count,
    COUNT(DISTINCT tariff) AS distinct_tariffs,
    MIN(seq_no) AS min_seq_no
FROM outliers
GROUP BY CUBE(seq_no_2, seq_no, tariff)
ORDER BY seq_no_2 NULLS LAST, seq_no NULLS LAST, tariff NULLS LAST
