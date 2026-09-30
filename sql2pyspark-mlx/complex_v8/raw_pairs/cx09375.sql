WITH prepared AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        commissioned_on,
        CONCAT_WS(' / ', tariff, zone) AS tariff_zone_label
    FROM sites
    WHERE LENGTH(TRIM(site_name)) >= 8
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.id) AS reading_count
    FROM prepared s
),
rollup_base AS (
    -- Summarise per group
    SELECT
        reading_count,
        DATE_TRUNC('MONTH', commissioned_on) AS commissioned_on_month,
        COUNT(DISTINCT tariff) AS distinct_tariffs,
        SUM(CASE WHEN tariff = 'TOU' THEN reading_count ELSE 0 END) AS reading_count_tariff_tou,
        MAX(reading_count) AS max_reading_count,
        COUNT(DISTINCT zone) AS distinct_zones
    FROM with_stats
    GROUP BY reading_count, DATE_TRUNC('MONTH', commissioned_on)
    HAVING AVG(reading_count) > 3
),
derived AS (
    -- Derive helper columns
    SELECT
        reading_count,
        commissioned_on_month,
        distinct_zones,
        distinct_tariffs + reading_count_tariff_tou AS distinct_tariffs_plus_reading_count_tariff_tou,
        DATE_TRUNC('DAY', commissioned_on_month) AS commissioned_on_month_day
    FROM rollup_base
),
derived_2 AS (
    -- Derive helper columns
    SELECT *, distinct_zones + reading_count AS distinct_zones_plus_reading_count
    FROM derived
    WHERE distinct_zones < 7
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(distinct_zones) IGNORE NULLS OVER (PARTITION BY YEAR(commissioned_on_month_day) ORDER BY distinct_tariffs_plus_reading_count_tariff_tou DESC, reading_count, commissioned_on_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_distinct_zones,
        ROUND(AVG(distinct_zones) OVER (PARTITION BY MONTH(commissioned_on_month_day), distinct_zones), 2) AS commissioned_on_month_day_month_distinct_zones_a,
        DENSE_RANK() OVER (PARTITION BY YEAR(commissioned_on_month_day) ORDER BY distinct_tariffs_plus_reading_count_tariff_tou DESC) AS distinct_tariffs_plus_reading_count_tariff_tou_d,
        COUNT(*) OVER (PARTITION BY MONTH(commissioned_on_month_day), distinct_zones) AS rows_in_commissioned_on_month_day_month_distinct,
        LEAD(commissioned_on_month_day) OVER (PARTITION BY reading_count ORDER BY distinct_zones DESC, reading_count, commissioned_on_month) AS next_commissioned_on_month_day,
        ROUND(reading_count - AVG(reading_count) OVER (PARTITION BY MONTH(commissioned_on_month_day), distinct_zones), 2) AS reading_count_vs_commissioned_on_month_day_month
    FROM derived_2
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        YEAR(commissioned_on_month) AS commissioned_on_month_year,
        reading_count - distinct_zones AS reading_count_minus_distinct_zones
    FROM windowed
)
-- Final projection
SELECT
    reading_count,
    commissioned_on_month,
    distinct_zones,
    commissioned_on_month_day,
    distinct_zones_plus_reading_count,
    first_known_distinct_zones,
    commissioned_on_month_day_month_distinct_zones_a,
    distinct_tariffs_plus_reading_count_tariff_tou_d,
    rows_in_commissioned_on_month_day_month_distinct,
    next_commissioned_on_month_day,
    commissioned_on_month_year,
    reading_count_minus_distinct_zones,
    commissioned_on_month + INTERVAL 7 DAYS AS commissioned_on_month_plus_7d
FROM labeled
