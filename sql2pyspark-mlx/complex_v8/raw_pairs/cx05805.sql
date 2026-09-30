WITH scoped_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw
    FROM sites
    WHERE commissioned_on < DATE '2024-11-21'
),
tariff_stats AS (
    -- Max installed_kw per tariff
    SELECT tariff, MAX(installed_kw) AS max_installed_kw
    FROM scoped_sites
    GROUP BY tariff
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_installed_kw,
        ROUND(s.installed_kw - g.max_installed_kw, 2) AS installed_kw_minus_max
    FROM scoped_sites s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.installed_kw = g.max_installed_kw
),
zone_stats AS (
    -- Avg max_installed_kw per zone
    SELECT zone, AVG(max_installed_kw) AS avg_max_installed_kw
    FROM above_average
    GROUP BY zone
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_max_installed_kw,
        ROUND(s.max_installed_kw - g.avg_max_installed_kw, 2) AS max_installed_kw_minus_avg
    FROM above_average s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.max_installed_kw > g.avg_max_installed_kw
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY zone ORDER BY max_installed_kw_minus_avg) AS max_installed_kw_minus_avg_rank,
        FIRST_VALUE(installed_kw_minus_max) OVER (ORDER BY max_installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_installed_kw_minus_max
    FROM outliers
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.read_at < TIMESTAMP '2025-03-08 10:00:00'
        )
      AND s.max_installed_kw > 203.0
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        max_installed_kw,
        installed_kw_minus_max,
        avg_max_installed_kw,
        max_installed_kw_minus_avg,
        max_installed_kw_minus_avg_rank,
        first_installed_kw_minus_max,
        ROUND(installed_kw - AVG(installed_kw) OVER w_zone, 2) AS installed_kw_vs_zone_avg,
        LAG(installed_kw) OVER w_tariff_by_max_installed_kw_m AS prev_installed_kw,
        COUNT(*) OVER (PARTITION BY zone) AS rows_in_zone,
        max_installed_kw_minus_avg - LAG(max_installed_kw_minus_avg) OVER (ORDER BY installed_kw_minus_max DESC, id) AS max_installed_kw_minus_avg_change
    FROM with_activity
    WINDOW w_zone AS (PARTITION BY zone), w_tariff_by_max_installed_kw_m AS (PARTITION BY tariff ORDER BY max_installed_kw_minus_avg_rank, id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        max_installed_kw,
        avg_max_installed_kw,
        max_installed_kw_minus_avg,
        max_installed_kw_minus_avg_rank,
        first_installed_kw_minus_max,
        installed_kw_vs_zone_avg,
        prev_installed_kw,
        rows_in_zone,
        max_installed_kw_minus_avg_change,
        FIRST_VALUE(rows_in_zone) IGNORE NULLS OVER (ORDER BY first_installed_kw_minus_max DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rows_in_zone,
        LAST_VALUE(first_installed_kw_minus_max) IGNORE NULLS OVER (ORDER BY prev_installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_installed_kw_minus_max,
        RANK() OVER w_all_by_first_installed_kw_mi_2 AS first_installed_kw_minus_max_rank,
        LAST_VALUE(installed_kw_vs_zone_avg) IGNORE NULLS OVER (ORDER BY prev_installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_installed_kw_vs_zone_avg
    FROM sequenced
    WINDOW w_all_by_first_installed_kw_mi_2 AS (ORDER BY first_installed_kw_minus_max DESC NULLS FIRST)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, reading
FROM (
    SELECT id, first_installed_kw_minus_max, installed_kw
    FROM with_trends
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (first_installed_kw_minus_max, installed_kw)
) unp
ORDER BY id, metric_name
