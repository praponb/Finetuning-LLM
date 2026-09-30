WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage BETWEEN 215.6 AND 234.5
      AND read_at >= TIMESTAMP '2025-02-06 08:00:00'
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        si.tariff AS site_tariff
    FROM base s
    JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        site_tariff,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY site_id, site_tariff ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        DENSE_RANK() OVER (ORDER BY kwh ASC) AS kwh_dense_rank,
        LAG(kwh, 2, 0) OVER (ORDER BY kwh ASC, id) AS prev2_kwh,
        ROW_NUMBER() OVER (ORDER BY kwh ASC, id) AS seq_no,
        MAX(kwh) OVER (PARTITION BY site_tariff, site_id) AS site_tariff_site_id_max_kwh,
        LAG(kwh, 1, 0) OVER w_all_by_kwh AS prev_kwh,
        ROUND(PERCENT_RANK() OVER (ORDER BY kwh), 3) AS kwh_percent_rank,
        MAX(voltage) OVER (PARTITION BY site_tariff, site_id) AS site_tariff_site_id_max_voltage,
        ROUND(CUME_DIST() OVER (ORDER BY kwh ASC, id), 3) AS kwh_cume_dist,
        LAST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY site_id, site_tariff ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        ROUND(AVG(voltage) OVER (ORDER BY kwh ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_7,
        RANK() OVER (ORDER BY kwh) AS kwh_rank,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_tariff, site_id), 2) AS kwh_vs_site_tariff_site_id_avg
    FROM with_parent
    WINDOW w_all_by_kwh AS (ORDER BY kwh ASC, id)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        kwh_rank,
        site_tariff,
        PERCENTILE_APPROX(kwh_vs_site_tariff_site_id_avg, 0.5) AS median_kwh_vs_site_tariff_site_id_avg,
        SUM(CASE WHEN site_tariff IN ('FLAT', 'EV') THEN second_kwh ELSE 0 END) AS second_kwh_site_tariff_in_flat_ev,
        ARRAY_SORT(COLLECT_SET(site_tariff)) AS site_tariff_list,
        ROUND(100 * COUNT(CASE WHEN site_tariff = 'EV' THEN 1 END) / COUNT(*), 1) AS pct_site_tariff_ev,
        ROUND(AVG(kwh_rank), 2) AS avg_kwh_rank
    FROM with_trends
    GROUP BY 1, 2
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER w_site_tariff_kwh_rank AS rows_in_site_tariff_kwh_rank,
        NTILE(10) OVER w_site_tariff_kwh_rank_by_avg AS avg_kwh_rank_decile
    FROM rollup_base
    WINDOW w_site_tariff_kwh_rank AS (PARTITION BY site_tariff, kwh_rank), w_site_tariff_kwh_rank_by_avg AS (PARTITION BY site_tariff, kwh_rank ORDER BY avg_kwh_rank, kwh_rank, site_tariff)
)
SELECT
    kwh_rank,
    site_tariff,
    median_kwh_vs_site_tariff_site_id_avg,
    second_kwh_site_tariff_in_flat_ev,
    site_tariff_list,
    avg_kwh_rank,
    rows_in_site_tariff_kwh_rank,
    avg_kwh_rank_decile,
    ABS(kwh_rank - 5) AS kwh_rank_distance,
    SIZE(site_tariff_list) AS site_tariff_list_count
FROM metrics
ORDER BY avg_kwh_rank DESC
