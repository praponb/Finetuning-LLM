WITH base AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        commissioned_on,
        DATE_TRUNC('MONTH', commissioned_on) AS commissioned_on_month,
        DATEDIFF(DATE '2025-12-31', commissioned_on) AS days_since_commissioned_on,
        YEAR(commissioned_on) AS commissioned_on_year
    FROM sites
    WHERE (UPPER(TRIM(site_name)) LIKE 'D%' OR tariff <> 'FLAT')
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM base
    WHERE commissioned_on_year > (SELECT MIN(commissioned_on_year) FROM base)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(commissioned_on, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', commissioned_on) ORDER BY commissioned_on_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_commissioned_on,
        FIRST_VALUE(commissioned_on) OVER (PARTITION BY DATE_TRUNC('MONTH', commissioned_on) ORDER BY commissioned_on_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_commissioned_on
    FROM qualified
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_name,
        tariff,
        commissioned_on,
        commissioned_on_month,
        days_since_commissioned_on,
        commissioned_on_year,
        second_commissioned_on,
        MAX(days_since_commissioned_on) OVER (PARTITION BY YEAR(first_commissioned_on)) AS first_commissioned_on_year_max_days_since_commis,
        SIZE(COLLECT_SET(zone) OVER (PARTITION BY YEAR(first_commissioned_on))) AS distinct_zone_in_first_commissioned_on_year,
        SUM(days_since_commissioned_on) OVER (ORDER BY commissioned_on, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS days_since_commissioned_on_sum_2,
        ROUND(days_since_commissioned_on - AVG(days_since_commissioned_on) OVER (PARTITION BY YEAR(first_commissioned_on)), 2) AS days_since_commissioned_on_vs_first_commissioned,
        SIZE(COLLECT_SET(tariff) OVER (PARTITION BY YEAR(first_commissioned_on))) AS distinct_tariff_in_first_commissioned_on_year
    FROM metrics
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at
    FROM with_windows s
    LEFT JOIN reading_stats m ON m.site_id = s.id
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS('-', tariff, site_name) AS tariff_site_name_label,
        ROUND((UNIX_TIMESTAMP(commissioned_on_month) - UNIX_TIMESTAMP(last_reading_read_at)) / 60, 1) AS minutes_last_reading_read_at_to_commissioned_on_
    FROM with_stats
    WHERE second_commissioned_on >= DATE '2024-10-28'
)
-- Final projection
SELECT
    id,
    site_name,
    tariff,
    commissioned_on,
    commissioned_on_year,
    second_commissioned_on,
    first_commissioned_on_year_max_days_since_commis,
    distinct_zone_in_first_commissioned_on_year,
    days_since_commissioned_on_sum_2,
    days_since_commissioned_on_vs_first_commissioned,
    last_reading_read_at,
    tariff_site_name_label,
    minutes_last_reading_read_at_to_commissioned_on_,
    CASE WHEN tariff IN ('EV', 'TOU') THEN 'group_a' WHEN tariff = 'FLAT' THEN 'group_b' ELSE 'other' END AS tariff_group
FROM labeled
ORDER BY second_commissioned_on, id
LIMIT 10
