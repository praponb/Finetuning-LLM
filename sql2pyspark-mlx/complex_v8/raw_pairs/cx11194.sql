WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE MONTH(read_at) IN (2, 3)
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.quality_slug,
        s.quality_or_unknown,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM recent s
    LEFT JOIN sites si ON si.id = s.site_id
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', site_commissioned_on) AS site_commissioned_on_month,
        quality_or_unknown,
        site_zone,
        COUNT(*) FILTER (WHERE site_zone IS NOT NULL) AS n_site_zone_is_not_null,
        ROUND(SUM(kwh) / NULLIF(SUM(voltage), 0), 3) AS kwh_to_voltage_ratio,
        COUNT(*) AS cnt
    FROM joined_site
    WHERE voltage <= 236.7
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(kwh_to_voltage_ratio * 1.1, 2) AS kwh_to_voltage_ratio_adj,
        CASE WHEN site_zone IN ('Z3', 'Z2') THEN 'group_a' WHEN site_zone = 'Z4' THEN 'group_b' ELSE 'other' END AS site_zone_group,
        LOWER(TRIM(quality_or_unknown)) AS quality_or_unknown_clean
    FROM per_group
    WHERE site_zone = 'Z2'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(site_zone_group) OVER (PARTITION BY n_site_zone_is_not_null, quality_or_unknown)) AS distinct_site_zone_group_in_n_site_zone_is_not_n,
        ROUND(AVG(cnt) OVER w_n_site_zone_is_not_null_qual, 2) AS n_site_zone_is_not_null_quality_or_unknown_avg_c,
        ROUND(n_site_zone_is_not_null / NULLIF(SUM(n_site_zone_is_not_null) OVER (PARTITION BY n_site_zone_is_not_null, quality_or_unknown), 0), 4) AS n_site_zone_is_not_null_share_of_n_site_zone_is_,
        NTH_VALUE(n_site_zone_is_not_null, 2) OVER (PARTITION BY cnt, n_site_zone_is_not_null ORDER BY kwh_to_voltage_ratio, site_commissioned_on_month, quality_or_unknown, site_zone ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_site_zone_is_not_null
    FROM tagged
    WINDOW w_n_site_zone_is_not_null_qual AS (PARTITION BY n_site_zone_is_not_null, quality_or_unknown)
),
derived AS (
    -- Label and score rows
    SELECT
        site_commissioned_on_month,
        quality_or_unknown,
        site_zone,
        n_site_zone_is_not_null,
        kwh_to_voltage_ratio,
        cnt,
        quality_or_unknown_clean,
        distinct_site_zone_group_in_n_site_zone_is_not_n,
        n_site_zone_is_not_null_quality_or_unknown_avg_c,
        n_site_zone_is_not_null_share_of_n_site_zone_is_,
        cnt + kwh_to_voltage_ratio_adj AS cnt_plus_kwh_to_voltage_ratio_adj,
        FLOOR(kwh_to_voltage_ratio_adj / 2) * 2 AS kwh_to_voltage_ratio_adj_bucket
    FROM sequenced
),
tagged_2 AS (
    -- Derive helper columns
    SELECT
        site_commissioned_on_month,
        quality_or_unknown,
        site_zone,
        n_site_zone_is_not_null,
        kwh_to_voltage_ratio,
        cnt,
        quality_or_unknown_clean,
        distinct_site_zone_group_in_n_site_zone_is_not_n,
        n_site_zone_is_not_null_quality_or_unknown_avg_c,
        n_site_zone_is_not_null_share_of_n_site_zone_is_,
        kwh_to_voltage_ratio_adj_bucket,
        FLOOR(cnt / 2) * 2 AS cnt_bucket
    FROM derived
)
-- Final projection
SELECT
    site_commissioned_on_month,
    quality_or_unknown,
    site_zone,
    n_site_zone_is_not_null,
    cnt,
    quality_or_unknown_clean,
    n_site_zone_is_not_null_quality_or_unknown_avg_c,
    n_site_zone_is_not_null_share_of_n_site_zone_is_,
    kwh_to_voltage_ratio_adj_bucket,
    kwh_to_voltage_ratio + cnt_bucket AS kwh_to_voltage_ratio_plus_cnt_bucket,
    COALESCE(site_zone, 'UNKNOWN') AS site_zone_or_unknown
FROM tagged_2
WHERE LENGTH(TRIM(quality_or_unknown_clean)) >= 4
