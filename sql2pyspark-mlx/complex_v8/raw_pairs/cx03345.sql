WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        CASE WHEN kwh >= 79.4 THEN 'HIGH' WHEN kwh >= 48.2 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier
    FROM meter_readings
    WHERE voltage < 240.2
      AND site_id IS NOT NULL
),
with_parent_site AS (
    -- Attach site attributes
    SELECT s.*, si.zone AS site_zone, si.tariff AS site_tariff
    FROM cleaned s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        quality_ref,
        kwh_tier,
        site_zone,
        site_tariff,
        FIRST_VALUE(kwh) OVER (PARTITION BY MONTH(read_at), site_id ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_zone), 0), 4) AS kwh_share_of_site_zone,
        ROUND(kwh - AVG(kwh) OVER w_site_tariff, 2) AS kwh_vs_site_tariff_avg,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id ORDER BY read_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_3,
        SUM(kwh) OVER (PARTITION BY site_tariff) AS site_tariff_sum_kwh,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (ORDER BY read_at DESC, id))) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(AVG(kwh) OVER (PARTITION BY MONTH(read_at), site_id ORDER BY kwh ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_2_prior,
        MIN(kwh) OVER (PARTITION BY quality ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_so_far,
        SIZE(COLLECT_SET(site_tariff) OVER (PARTITION BY site_zone)) AS distinct_site_tariff_in_site_zone
    FROM with_parent_site
    WINDOW w_site_tariff AS (PARTITION BY site_tariff)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        kwh_tier,
        site_zone,
        site_tariff,
        ROUND(AVG(min_kwh_so_far), 2) AS avg_min_kwh_so_far,
        ROUND(AVG(site_tariff_sum_kwh), 2) AS avg_site_tariff_sum_kwh,
        MAX(site_tariff_sum_kwh) AS max_site_tariff_sum_kwh,
        COUNT(*) FILTER (WHERE site_zone = 'Z3') AS n_site_zone_z3,
        COUNT(*) AS cnt
    FROM with_trends
    GROUP BY kwh_tier, site_zone, site_tariff
)
SELECT
    kwh_tier,
    site_zone,
    site_tariff,
    avg_site_tariff_sum_kwh,
    max_site_tariff_sum_kwh,
    n_site_zone_z3,
    cnt
FROM totals
ORDER BY avg_site_tariff_sum_kwh DESC
