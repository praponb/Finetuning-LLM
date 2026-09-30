WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 0.9, 2) AS kwh_adj
    FROM meter_readings
    WHERE quality IS NOT NULL
),
enriched_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.kwh_adj,
        si.tariff AS site_tariff
    FROM filtered s
    LEFT JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        kwh_adj,
        site_tariff,
        SUM(voltage) OVER (PARTITION BY site_tariff, quality) AS site_tariff_quality_sum_voltage,
        LEAD(kwh_adj, 3, 0) OVER w_all_by_kwh AS next3_kwh_adj,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY site_tariff, quality)) AS distinct_site_id_in_site_tariff_quality,
        SUM(kwh_adj) OVER (PARTITION BY site_id) AS site_id_sum_kwh_adj,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id), 2) AS site_id_avg_kwh,
        DENSE_RANK() OVER (PARTITION BY site_id ORDER BY voltage ASC, id) AS voltage_dense_rank,
        kwh - LAG(kwh) OVER (ORDER BY voltage DESC, id) AS kwh_change,
        ROW_NUMBER() OVER (ORDER BY voltage DESC, id) AS voltage_seq,
        LEAD(kwh) OVER (ORDER BY kwh DESC, id) AS next_kwh
    FROM enriched_site
    WINDOW w_all_by_kwh AS (ORDER BY kwh DESC, id)
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        kwh_adj,
        site_tariff,
        site_tariff_quality_sum_voltage,
        next3_kwh_adj,
        distinct_site_id_in_site_tariff_quality,
        site_id_sum_kwh_adj,
        site_id_avg_kwh,
        kwh_change,
        voltage_seq,
        next_kwh,
        ROUND(AVG(next3_kwh_adj) OVER (PARTITION BY quality, distinct_site_id_in_site_tariff_quality ORDER BY kwh_change DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS next3_kwh_adj_avg_2_prior,
        ROUND(site_id_sum_kwh_adj - AVG(site_id_sum_kwh_adj) OVER w_site_id_site_tariff, 2) AS site_id_sum_kwh_adj_vs_site_id_site_tariff_avg,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id, site_tariff)) AS distinct_quality_in_site_id_site_tariff,
        DENSE_RANK() OVER (PARTITION BY quality, distinct_site_id_in_site_tariff_quality ORDER BY kwh_change DESC NULLS FIRST) AS kwh_change_dense_rank
    FROM metrics
    WINDOW w_site_id_site_tariff AS (PARTITION BY site_id, site_tariff)
)
SELECT
    id,
    read_at,
    voltage,
    quality,
    site_tariff_quality_sum_voltage,
    next3_kwh_adj,
    distinct_site_id_in_site_tariff_quality,
    site_id_sum_kwh_adj,
    site_id_avg_kwh,
    kwh_change,
    next_kwh,
    next3_kwh_adj_avg_2_prior,
    site_id_sum_kwh_adj_vs_site_id_site_tariff_avg,
    kwh_change_dense_rank,
    SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
    FLOOR(kwh_change / 50) * 50 AS kwh_change_bucket
FROM metrics_2
WHERE site_id_avg_kwh < 78.0
ORDER BY quality_prefix NULLS LAST, id
LIMIT 10
