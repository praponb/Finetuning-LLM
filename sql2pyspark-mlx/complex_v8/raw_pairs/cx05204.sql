WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        ROUND(voltage * 1.5, 2) AS voltage_adj,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.voltage_adj,
        si.tariff AS site_tariff
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY voltage_adj DESC, id) AS voltage_adj_dense_rank,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality,
        MAX(voltage_adj) OVER (PARTITION BY site_tariff, quality) AS site_tariff_quality_max_voltage_adj,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY site_tariff, quality)) AS distinct_site_id_in_site_tariff_quality,
        ROUND(AVG(kwh) OVER w_site_tariff_quality, 2) AS site_tariff_quality_avg_kwh,
        SIZE(COLLECT_SET(quality) OVER w_site_tariff_site_id) AS distinct_quality_in_site_tariff_site_id,
        LEAD(read_at, 3) OVER w_quality_by_kwh AS next3_read_at,
        MIN(kwh) OVER (PARTITION BY quality, site_tariff ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_so_far,
        SUM(voltage_adj) OVER (PARTITION BY site_tariff, site_id) AS site_tariff_site_id_sum_voltage_adj
    FROM joined
    WINDOW w_site_tariff_quality AS (PARTITION BY site_tariff, quality), w_site_tariff_site_id AS (PARTITION BY site_tariff, site_id), w_quality_by_kwh AS (PARTITION BY quality ORDER BY kwh ASC, id)
),
labeled AS (
    -- Derive helper columns
    SELECT *, ABS(kwh - 50.4) AS kwh_distance
    FROM metrics
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        quality,
        voltage_adj,
        voltage_adj_dense_rank,
        distinct_site_id_in_quality,
        site_tariff_quality_max_voltage_adj,
        distinct_site_id_in_site_tariff_quality,
        site_tariff_quality_avg_kwh,
        next3_read_at,
        min_kwh_so_far,
        site_tariff_site_id_sum_voltage_adj,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY site_id ORDER BY site_tariff_quality_max_voltage_adj ASC, id))) / 60, 1) AS minutes_since_prev_read_at,
        DENSE_RANK() OVER w_all_by_distinct_site_id_in_s AS distinct_site_id_in_site_tariff_quality_dense_ra,
        ROUND(AVG(voltage_adj_dense_rank) OVER (PARTITION BY voltage_adj_dense_rank, DATE_TRUNC('MONTH', read_at) ORDER BY min_kwh_so_far DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS voltage_adj_dense_rank_avg_3_prior,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY voltage_adj_dense_rank, DATE_TRUNC('MONTH', read_at) ORDER BY min_kwh_so_far DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh
    FROM labeled
    WINDOW w_all_by_distinct_site_id_in_s AS (ORDER BY distinct_site_id_in_site_tariff_quality, id)
)
SELECT
    id,
    site_id,
    voltage_adj_dense_rank,
    distinct_site_id_in_quality,
    site_tariff_quality_avg_kwh,
    next3_read_at,
    min_kwh_so_far,
    site_tariff_site_id_sum_voltage_adj,
    minutes_since_prev_read_at,
    distinct_site_id_in_site_tariff_quality_dense_ra,
    voltage_adj_dense_rank_avg_3_prior,
    second_kwh
FROM windowed
WHERE minutes_since_prev_read_at > 1500.0
ORDER BY minutes_since_prev_read_at NULLS LAST
