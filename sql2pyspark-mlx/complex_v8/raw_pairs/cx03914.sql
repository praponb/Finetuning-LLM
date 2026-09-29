WITH valid_sites AS (
    -- Scope the sites rows
    SELECT *
    FROM sites
    WHERE commissioned_on >= DATE '2024-10-28'
      AND LENGTH(TRIM(site_name)) >= 8
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        site_name,
        zone,
        tariff,
        commissioned_on,
        ROUND(PERCENT_RANK() OVER (PARTITION BY zone, tariff ORDER BY installed_kw ASC), 3) AS installed_kw_percent_rank,
        LAST_VALUE(installed_kw) IGNORE NULLS OVER (PARTITION BY zone, tariff ORDER BY installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_installed_kw,
        SUM(installed_kw) OVER w_zone_tariff AS zone_tariff_sum_installed_kw,
        LAG(commissioned_on) OVER (PARTITION BY zone ORDER BY commissioned_on ASC, id) AS prev_commissioned_on,
        ROUND(installed_kw / NULLIF(SUM(installed_kw) OVER w_zone_tariff, 0), 4) AS installed_kw_share_of_zone_tariff,
        FIRST_VALUE(commissioned_on) OVER (PARTITION BY zone, tariff ORDER BY installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_commissioned_on
    FROM valid_sites
    WINDOW w_zone_tariff AS (PARTITION BY zone, tariff)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        YEAR(prev_commissioned_on) AS prev_commissioned_on_year,
        CASE WHEN tariff IN ('EV', 'TOU') THEN 'group_a' WHEN tariff = 'FLAT' THEN 'group_b' ELSE 'other' END AS tariff_group,
        DAYOFWEEK(commissioned_on) AS commissioned_on_dayofweek
    FROM ranked
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM tagged s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
        )
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_name,
        s.zone,
        s.commissioned_on,
        s.installed_kw_percent_rank,
        s.prev_commissioned_on,
        s.installed_kw_share_of_zone_tariff,
        s.prev_commissioned_on_year,
        s.commissioned_on_dayofweek,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh
    FROM screened s
    JOIN meter_readings m ON m.site_id = s.id
)
-- Final projection
SELECT
    id,
    site_name,
    zone,
    commissioned_on,
    prev_commissioned_on,
    installed_kw_share_of_zone_tariff,
    commissioned_on_dayofweek,
    reading_id,
    reading_kwh,
    LOWER(REGEXP_REPLACE(TRIM(site_name), ' ', '_')) AS site_name_slug
FROM with_children_readings
ORDER BY prev_commissioned_on, site_name, id, reading_id
LIMIT 20
