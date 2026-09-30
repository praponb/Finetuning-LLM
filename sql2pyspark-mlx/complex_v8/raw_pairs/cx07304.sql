WITH filtered_sites AS (
    -- Filter sites
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        DATEDIFF(DATE '2025-06-30', commissioned_on) AS days_since_commissioned_on
    FROM sites
    WHERE commissioned_on >= DATE '2024-09-15'
      AND LENGTH(TRIM(site_name)) >= 7
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        zone,
        commissioned_on,
        days_since_commissioned_on,
        COUNT(*) OVER w_tariff_zone AS rows_in_tariff_zone,
        SUM(days_since_commissioned_on) OVER (PARTITION BY tariff ORDER BY commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_commissioned_on,
        DENSE_RANK() OVER (PARTITION BY tariff ORDER BY days_since_commissioned_on ASC) AS days_since_commissioned_on_dense_rank,
        COUNT(*) OVER (PARTITION BY tariff ORDER BY days_since_commissioned_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(days_since_commissioned_on) OVER (PARTITION BY zone, tariff ORDER BY days_since_commissioned_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_commissioned_on_2,
        ROUND(installed_kw - AVG(installed_kw) OVER w_tariff_zone, 2) AS installed_kw_vs_tariff_zone_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY tariff ORDER BY commissioned_on DESC), 3) AS commissioned_on_cume_dist
    FROM filtered_sites
    WINDOW w_tariff_zone AS (PARTITION BY tariff, zone)
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM with_windows
    WHERE days_since_commissioned_on_dense_rank <= 3
)
-- Final projection
SELECT
    id,
    zone,
    commissioned_on,
    days_since_commissioned_on,
    rows_in_tariff_zone,
    running_days_since_commissioned_on,
    days_since_commissioned_on_dense_rank,
    running_count,
    running_days_since_commissioned_on_2,
    commissioned_on_cume_dist,
    LENGTH(TRIM(zone)) AS zone_len,
    DATE_FORMAT(commissioned_on, 'MMM yyyy') AS commissioned_on_label
FROM leaders
WHERE running_days_since_commissioned_on <= 4550
ORDER BY zone, rows_in_tariff_zone
