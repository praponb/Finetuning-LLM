WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-31 12:00:00'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_ref,
        LAST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY quality ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality, site_id), 2) AS kwh_vs_quality_site_id_avg,
        FIRST_VALUE(kwh) OVER (PARTITION BY quality, site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh
    FROM recent
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id)
),
summary AS (
    -- Group-level totals
    SELECT
        quality,
        site_id,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        SUM(kwh_vs_quality_site_id_avg) AS total_kwh_vs_quality_site_id_avg,
        COUNT(*) AS row_count,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        MAX(kwh_vs_quality_site_id_avg) AS max_kwh_vs_quality_site_id_avg,
        SUM(CASE WHEN quality IN ('EST', 'BAD') THEN 1 ELSE 0 END) AS n_quality_in_est_bad
    FROM metrics
    WHERE first_kwh > 16.5
    GROUP BY quality, site_id
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM summary s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(row_count) OVER (ORDER BY max_kwh_vs_quality_site_id_avg ASC NULLS LAST, quality, site_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS row_count_avg_3_prior,
        ROUND(distinct_qualitys - AVG(distinct_qualitys) OVER (PARTITION BY quality), 2) AS distinct_qualitys_vs_quality_avg,
        ROUND(AVG(distinct_qualitys) OVER (PARTITION BY quality), 2) AS quality_avg_distinct_qualitys,
        NTILE(10) OVER (ORDER BY max_kwh_vs_quality_site_id_avg ASC NULLS LAST, quality, site_id) AS max_kwh_vs_quality_site_id_avg_decile,
        RANK() OVER (ORDER BY max_kwh_vs_quality_site_id_avg ASC NULLS LAST) AS max_kwh_vs_quality_site_id_avg_rank,
        ROUND(row_count / NULLIF(SUM(row_count) OVER (PARTITION BY quality), 0), 4) AS row_count_share_of_quality
    FROM screened
),
with_details AS (
    -- Join sites
    SELECT
        s.quality,
        s.site_id,
        s.quality_list,
        s.total_kwh_vs_quality_site_id_avg,
        s.row_count,
        s.distinct_qualitys,
        s.distinct_qualitys_vs_quality_avg,
        s.quality_avg_distinct_qualitys,
        s.max_kwh_vs_quality_site_id_avg_decile,
        s.max_kwh_vs_quality_site_id_avg_rank,
        s.row_count_share_of_quality,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM sequenced s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff IS NOT NULL
)
SELECT
    quality,
    quality_list,
    row_count,
    distinct_qualitys,
    distinct_qualitys_vs_quality_avg,
    max_kwh_vs_quality_site_id_avg_rank,
    row_count_share_of_quality,
    site_tariff,
    site_installed_kw,
    site_zone
FROM with_details
ORDER BY row_count DESC
