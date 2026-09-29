WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE MONTH(read_at) IN (2, 3)
      AND quality <> 'EST'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        NTILE(4) OVER w_quality_by_kwh AS kwh_quartile,
        ROUND(AVG(kwh) OVER (PARTITION BY quality ORDER BY kwh NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_2,
        LAST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY quality ORDER BY kwh NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh,
        LEAD(read_at, 2) OVER (PARTITION BY quality ORDER BY kwh NULLS LAST, id) AS next2_read_at
    FROM base_readings
    WINDOW w_quality_by_kwh AS (PARTITION BY quality ORDER BY kwh NULLS LAST, id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY kwh_quartile, site_id ORDER BY next2_read_at) AS next2_read_at_rank,
        ROUND(kwh_quartile / NULLIF(SUM(kwh_quartile) OVER (PARTITION BY site_id, kwh_quartile), 0), 4) AS kwh_quartile_share_of_site_id_kwh_quartile
    FROM with_windows
),
enriched AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.next2_read_at,
        s.next2_read_at_rank,
        s.kwh_quartile_share_of_site_id_kwh_quartile,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff
    FROM metrics s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw < 373.3
),
unique_rows AS (
    -- Keep the latest row per quality, site_id
    SELECT
        id,
        site_id,
        kwh,
        quality,
        next2_read_at,
        next2_read_at_rank,
        kwh_quartile_share_of_site_id_kwh_quartile,
        site_commissioned_on,
        site_installed_kw,
        site_tariff
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY next2_read_at DESC, id DESC) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.next2_read_at,
        s.next2_read_at_rank,
        s.kwh_quartile_share_of_site_id_kwh_quartile,
        s.site_commissioned_on,
        s.site_installed_kw,
        s.site_tariff,
        si.site_name
    FROM unique_rows s
    LEFT JOIN sites si ON si.id = s.site_id
),
grouped AS (
    -- Summarise per group
    SELECT
        YEAR(site_commissioned_on) AS site_commissioned_on_year,
        site_id,
        COUNT(*) AS n_rows,
        MAX(site_commissioned_on) AS last_site_commissioned_on,
        ROUND(SUM(site_installed_kw) / NULLIF(SUM(kwh_quartile_share_of_site_id_kwh_quartile), 0), 3) AS site_installed_kw_to_kwh_quartile_share_of_site_,
        MIN(kwh_quartile_share_of_site_id_kwh_quartile) AS min_kwh_quartile_share_of_site_id_kwh_quartile,
        ROUND(AVG(next2_read_at_rank), 2) AS avg_next2_read_at_rank,
        COUNT(*) FILTER (WHERE UPPER(TRIM(site_name)) LIKE 'D%') AS n_upper_trim_site_name_like_d
    FROM enriched_site
    GROUP BY 1, 2
)
-- Final projection
SELECT
    site_commissioned_on_year,
    site_id,
    n_rows,
    last_site_commissioned_on,
    site_installed_kw_to_kwh_quartile_share_of_site_,
    min_kwh_quartile_share_of_site_id_kwh_quartile,
    avg_next2_read_at_rank,
    n_upper_trim_site_name_like_d,
    ROUND(avg_next2_read_at_rank * 0.9, 2) AS avg_next2_read_at_rank_adj,
    ROUND(avg_next2_read_at_rank * 0.9, 2) AS avg_next2_read_at_rank_adj_2
FROM grouped
WHERE n_rows <= 11
ORDER BY avg_next2_read_at_rank DESC, avg_next2_read_at_rank_adj_2
