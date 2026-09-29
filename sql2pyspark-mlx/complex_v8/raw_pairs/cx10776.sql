WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-25 11:00:00'
      AND voltage < 232.7
),
enriched AS (
    -- Join sites
    SELECT
        s.*,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff,
        si.site_name
    FROM valid s
    JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        site_commissioned_on,
        site_tariff,
        site_name,
        DENSE_RANK() OVER w_site_id_by_read_at AS read_at_dense_rank,
        ROW_NUMBER() OVER w_quality_site_tariff_by_read AS read_at_seq,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality), 2) AS kwh_vs_quality_avg,
        ROUND(PERCENT_RANK() OVER w_all_by_read_at_ties, 3) AS read_at_percent_rank,
        ROUND(AVG(kwh) OVER w_quality, 2) AS quality_avg_kwh,
        SUM(kwh) OVER (PARTITION BY quality, site_tariff ORDER BY read_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS kwh_sum_1_prior,
        LEAD(site_commissioned_on, 3) OVER (PARTITION BY site_id ORDER BY read_at NULLS LAST, id) AS next3_site_commissioned_on,
        SIZE(COLLECT_SET(site_tariff) OVER (PARTITION BY quality)) AS distinct_site_tariff_in_quality,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality,
        MIN(kwh) OVER (PARTITION BY quality) AS quality_min_kwh,
        RANK() OVER (ORDER BY read_at) AS read_at_rank,
        LAST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh,
        MAX(kwh) OVER (PARTITION BY site_id, site_tariff) AS site_id_site_tariff_max_kwh
    FROM enriched
    WINDOW w_site_id_by_read_at AS (PARTITION BY site_id ORDER BY read_at NULLS LAST, id), w_quality_site_tariff_by_read AS (PARTITION BY quality, site_tariff ORDER BY read_at, id), w_all_by_read_at_ties AS (ORDER BY read_at), w_quality AS (PARTITION BY quality)
)
SELECT
    id,
    site_id,
    kwh,
    quality,
    site_commissioned_on,
    site_tariff,
    site_name,
    read_at_seq,
    kwh_vs_quality_avg,
    read_at_percent_rank,
    quality_avg_kwh,
    kwh_sum_1_prior,
    next3_site_commissioned_on,
    distinct_site_tariff_in_quality,
    distinct_site_id_in_quality,
    quality_min_kwh,
    last_known_kwh,
    site_id_site_tariff_max_kwh,
    CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
FROM with_trends
ORDER BY id DESC
