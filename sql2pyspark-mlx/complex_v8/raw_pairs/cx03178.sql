WITH filtered_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN kwh >= 101.2 THEN 'A' WHEN kwh >= 45.0 THEN 'B' ELSE 'C' END AS kwh_tier
    FROM meter_readings
    WHERE kwh > 37.0
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone,
        si.site_name
    FROM filtered_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.site_name IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(kwh) OVER (PARTITION BY site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        ROUND(kwh - AVG(kwh) OVER w_site_id_read_at_month, 2) AS kwh_vs_site_id_read_at_month_avg,
        SUM(kwh) OVER w_site_zone AS site_zone_sum_kwh,
        RANK() OVER (PARTITION BY site_id, YEAR(read_at) ORDER BY kwh) AS kwh_rank,
        ROW_NUMBER() OVER w_all_by_kwh AS kwh_seq,
        LAG(read_at, 2) OVER w_all_by_kwh AS prev2_read_at,
        ROUND(voltage - AVG(voltage) OVER w_site_id_read_at_month, 2) AS voltage_vs_site_id_read_at_month_avg,
        FIRST_VALUE(site_installed_kw) OVER (ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_site_installed_kw,
        DENSE_RANK() OVER (PARTITION BY site_id, YEAR(read_at) ORDER BY kwh) AS kwh_dense_rank,
        LAST_VALUE(site_installed_kw) OVER (PARTITION BY site_id, YEAR(read_at) ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_site_installed_kw,
        NTH_VALUE(read_at, 2) OVER (ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        COUNT(*) OVER w_site_zone AS rows_in_site_zone,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY site_zone)) AS distinct_site_id_in_site_zone,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER (PARTITION BY site_zone), 0), 4) AS site_installed_kw_share_of_site_zone
    FROM with_parent_site
    WINDOW w_site_id_read_at_month AS (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at)), w_site_zone AS (PARTITION BY site_zone), w_all_by_kwh AS (ORDER BY kwh, id)
),
first_rows AS (
    -- Top 2 per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        site_installed_kw,
        site_zone,
        site_name,
        last_kwh,
        kwh_vs_site_id_read_at_month_avg,
        site_zone_sum_kwh,
        kwh_seq,
        prev2_read_at,
        voltage_vs_site_id_read_at_month_avg,
        first_site_installed_kw,
        kwh_dense_rank,
        last_site_installed_kw,
        second_read_at,
        rows_in_site_zone,
        distinct_site_id_in_site_zone,
        site_installed_kw_share_of_site_zone
    FROM sequenced
    WHERE kwh_rank <= 2
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(prev2_read_at, 'yyyy-MM') AS prev2_read_at_ym,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_ym,
        site_zone,
        SUM(CASE WHEN LOWER(site_name) LIKE '% a%' THEN site_installed_kw_share_of_site_zone ELSE 0 END) AS site_installed_kw_share_of_site_zone_lower_site_,
        MAX(prev2_read_at) AS last_prev2_read_at,
        MAX(second_read_at) AS last_second_read_at
    FROM first_rows
    GROUP BY DATE_FORMAT(prev2_read_at, 'yyyy-MM'), DATE_FORMAT(read_at, 'yyyy-MM'), site_zone
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        prev2_read_at_ym,
        read_at_ym,
        site_zone,
        site_installed_kw_share_of_site_zone_lower_site_,
        ROW_NUMBER() OVER (ORDER BY site_installed_kw_share_of_site_zone_lower_site_, prev2_read_at_ym, read_at_ym, site_zone) AS site_installed_kw_share_of_site_zone_lower_site__2,
        ROUND(CUME_DIST() OVER w_all_by_site_installed_kw_sha, 3) AS site_installed_kw_share_of_site_zone_lower_site__3
    FROM per_group
    WINDOW w_all_by_site_installed_kw_sha AS (ORDER BY site_installed_kw_share_of_site_zone_lower_site_, prev2_read_at_ym, read_at_ym, site_zone)
)
-- Final projection
SELECT
    prev2_read_at_ym,
    site_zone,
    site_installed_kw_share_of_site_zone_lower_site_,
    site_installed_kw_share_of_site_zone_lower_site__2,
    site_installed_kw_share_of_site_zone_lower_site__3,
    CASE WHEN site_zone IN ('Z3', 'Z4') THEN 'group_a' WHEN site_zone = 'Z2' THEN 'group_b' ELSE 'other' END AS site_zone_group
FROM metrics
ORDER BY prev2_read_at_ym DESC, site_zone_group NULLS LAST
