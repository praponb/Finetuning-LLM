WITH valid AS (
    -- Scope the sites rows
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        COALESCE(installed_kw, 0.0) AS installed_kw_filled,
        COALESCE(installed_kw, 0.0) AS installed_kw_filled_2,
        DATEDIFF(DATE '2025-06-30', commissioned_on) AS days_since_commissioned_on
    FROM sites
    WHERE site_name IS NOT NULL
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(zone, 'UNKNOWN') AS zone_or_unknown,
        SUBSTRING(TRIM(zone), 1, 4) AS zone_prefix
    FROM valid
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        tariff,
        installed_kw,
        days_since_commissioned_on,
        zone_prefix,
        ROUND(installed_kw_filled - AVG(installed_kw_filled) OVER (PARTITION BY zone_or_unknown, tariff), 2) AS installed_kw_filled_vs_zone_or_unknown_tariff_av,
        COUNT(*) OVER w_zone_or_unknown_tariff AS rows_in_zone_or_unknown_tariff,
        DENSE_RANK() OVER (PARTITION BY zone ORDER BY installed_kw_filled) AS installed_kw_filled_dense_rank,
        RANK() OVER (PARTITION BY zone ORDER BY installed_kw_filled) AS installed_kw_filled_rank,
        NTH_VALUE(days_since_commissioned_on, 2) OVER (PARTITION BY zone ORDER BY installed_kw_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_commissioned_on,
        ROUND(CUME_DIST() OVER (PARTITION BY zone ORDER BY installed_kw_filled), 3) AS installed_kw_filled_cume_dist
    FROM tagged
    WINDOW w_zone_or_unknown_tariff AS (PARTITION BY zone_or_unknown, tariff)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, installed_kw_filled_dense_rank, rows_in_zone_or_unknown_tariff, second_days_since_commissioned_on
    FROM metrics
) src
UNPIVOT (
    reading FOR field IN (installed_kw_filled_dense_rank AS m_installed_kw_filled_dense_rank, rows_in_zone_or_unknown_tariff AS rows_in_zone_or_unknown_tariff_v, second_days_since_commissioned_on AS second_days_since_commissioned_on_v)
) unp
