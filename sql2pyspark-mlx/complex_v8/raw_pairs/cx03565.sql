WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
),
joined AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM base s
    LEFT JOIN sites si ON si.id = s.site_id AND si.commissioned_on IS NOT NULL
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(kwh - AVG(kwh) OVER w_site_id_site_zone, 2) AS kwh_vs_site_id_site_zone_avg,
        SUM(kwh) OVER (PARTITION BY site_zone) AS site_zone_sum_kwh,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_zone), 0), 4) AS kwh_share_of_site_zone,
        ROW_NUMBER() OVER (ORDER BY kwh DESC, id) AS kwh_seq,
        LAST_VALUE(kwh) OVER (ORDER BY site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh
    FROM joined
    WINDOW w_site_id_site_zone AS (PARTITION BY site_id, site_zone)
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        site_commissioned_on,
        site_zone,
        kwh_vs_site_id_site_zone_avg,
        site_zone_sum_kwh,
        kwh_share_of_site_zone,
        last_kwh
    FROM with_trends
    WHERE kwh_seq <= 3
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        site_zone,
        SUM(kwh_vs_site_id_site_zone_avg) AS total_kwh_vs_site_id_site_zone_avg,
        ROUND(SUM(last_kwh) / NULLIF(SUM(site_zone_sum_kwh), 0), 3) AS last_kwh_to_site_zone_sum_kwh_ratio,
        MIN(kwh_vs_site_id_site_zone_avg) AS min_kwh_vs_site_id_site_zone_avg,
        MAX(kwh) AS max_kwh
    FROM first_rows
    WHERE site_commissioned_on >= DATE '2024-08-24'
    GROUP BY 1, 2
    HAVING AVG(kwh_vs_site_id_site_zone_avg) > -96.0
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        read_at_month,
        site_zone,
        total_kwh_vs_site_id_site_zone_avg,
        last_kwh_to_site_zone_sum_kwh_ratio,
        FIRST_VALUE(max_kwh) IGNORE NULLS OVER (PARTITION BY site_zone ORDER BY min_kwh_vs_site_id_site_zone_avg DESC, read_at_month, site_zone ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_kwh,
        ROW_NUMBER() OVER (PARTITION BY site_zone ORDER BY min_kwh_vs_site_id_site_zone_avg DESC, read_at_month, site_zone) AS rn,
        NTH_VALUE(max_kwh, 2) OVER (PARTITION BY site_zone ORDER BY min_kwh_vs_site_id_site_zone_avg DESC, read_at_month, site_zone ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_max_kwh
    FROM stats
)
-- Final projection
SELECT
    read_at_month,
    site_zone,
    first_known_max_kwh,
    rn,
    second_max_kwh,
    UPPER(TRIM(site_zone)) AS site_zone_clean,
    DATE_TRUNC('YEAR', read_at_month) AS read_at_month_year
FROM with_windows
