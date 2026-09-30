WITH base AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw
    FROM sites
    WHERE installed_kw >= 96.9
      AND tariff IN ('TOU', 'EV')
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        INITCAP(TRIM(tariff)) AS tariff_clean,
        CONCAT(site_name, '#', CAST(id AS STRING)) AS site_name_ref,
        CASE WHEN installed_kw >= 373.3 THEN 'HIGH' WHEN installed_kw >= 96.9 THEN 'MEDIUM' ELSE 'LOW' END AS installed_kw_tier
    FROM base
    WHERE zone NOT IN ('Z3', 'Z4')
),
annotated_2 AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket,
        CONCAT_WS('|', installed_kw_tier, tariff_clean) AS installed_kw_tier_tariff_clean_label
    FROM annotated
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        site_name_ref,
        installed_kw_tier,
        installed_kw_bucket,
        installed_kw_tier_tariff_clean_label,
        NTH_VALUE(installed_kw, 2) OVER (PARTITION BY zone, installed_kw_tier ORDER BY installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_installed_kw,
        ROUND(AVG(installed_kw_bucket) OVER w_installed_kw_tier, 2) AS installed_kw_tier_avg_installed_kw_bucket,
        LAG(installed_kw_bucket) OVER w_installed_kw_tier_by_install AS prev_installed_kw_bucket,
        LEAD(installed_kw) OVER w_installed_kw_tier_by_install AS next_installed_kw,
        LAST_VALUE(installed_kw_bucket) IGNORE NULLS OVER (PARTITION BY zone, installed_kw_tier ORDER BY installed_kw, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_installed_kw_bucket
    FROM annotated_2
    WINDOW w_installed_kw_tier AS (PARTITION BY installed_kw_tier), w_installed_kw_tier_by_install AS (PARTITION BY installed_kw_tier ORDER BY installed_kw, id)
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(voltage) AS min_reading_voltage, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.min_reading_voltage,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM with_windows s
    LEFT JOIN reading_stats m ON m.site_id = s.id
),
totals AS (
    -- Group-level totals
    SELECT
        reading_count,
        zone,
        ROUND(SUM(next_installed_kw) / NULLIF(SUM(installed_kw_tier_avg_installed_kw_bucket), 0), 3) AS next_installed_kw_to_installed_kw_tier_avg_insta,
        ARRAY_SORT(COLLECT_SET(zone)) AS zone_list,
        ROUND(SUM(next_installed_kw), 2) AS total_next_installed_kw,
        ARRAY_SORT(COLLECT_SET(site_name)) AS site_name_list,
        MIN(prev_installed_kw_bucket) AS min_prev_installed_kw_bucket
    FROM summarized_rows
    WHERE installed_kw_tier_tariff_clean_label IS NOT NULL
    GROUP BY reading_count, zone
),
per_label AS (
    -- One row per site_name_list_item
    SELECT
        s.reading_count,
        s.zone,
        s.next_installed_kw_to_installed_kw_tier_avg_insta,
        s.zone_list,
        s.total_next_installed_kw,
        s.min_prev_installed_kw_bucket,
        t.site_name_list_item
    FROM totals s
    LATERAL VIEW EXPLODE(s.site_name_list) t AS site_name_list_item
)
SELECT
    reading_count,
    zone,
    next_installed_kw_to_installed_kw_tier_avg_insta,
    zone_list,
    total_next_installed_kw,
    min_prev_installed_kw_bucket,
    ARRAY_JOIN(ARRAY_SORT(zone_list), ',') AS zone_list_text,
    CASE WHEN zone IN ('Z4', 'Z3') THEN 'group_a' WHEN zone = 'Z2' THEN 'group_b' ELSE 'other' END AS zone_group
FROM per_label
WHERE total_next_installed_kw BETWEEN 604.0 AND 2401.0
ORDER BY zone_list_text, min_prev_installed_kw_bucket DESC
