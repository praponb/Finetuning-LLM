WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE MONTH(read_at) IN (1, 3)
      AND quality IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(voltage, 3, 0) OVER (PARTITION BY site_id, YEAR(read_at) ORDER BY read_at DESC, id) AS next3_voltage,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        DENSE_RANK() OVER w_all_by_kwh_ties AS kwh_dense_rank,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        RANK() OVER (ORDER BY kwh NULLS LAST) AS kwh_rank,
        MAX(voltage) OVER (PARTITION BY site_id, quality ORDER BY read_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS voltage_max_3,
        ROUND(PERCENT_RANK() OVER (ORDER BY kwh NULLS LAST), 3) AS kwh_percent_rank,
        MAX(kwh) OVER (PARTITION BY site_id, quality ORDER BY read_at ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS kwh_max_3_prior,
        ROUND(AVG(kwh) OVER w_site_id, 2) AS site_id_avg_kwh,
        RANK() OVER (PARTITION BY site_id, quality ORDER BY read_at ASC) AS read_at_rank
    FROM recent_readings
    WINDOW w_all_by_kwh_ties AS (ORDER BY kwh NULLS LAST), w_site_id AS (PARTITION BY site_id)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        next3_voltage,
        kwh_dense_rank,
        kwh_percent_rank,
        site_id_avg_kwh,
        read_at_rank,
        kwh_rank - rows_in_site_id AS kwh_rank_minus_rows_in_site_id,
        ROUND(kwh_rank / NULLIF(next3_voltage, 0), 3) AS kwh_rank_per_next3_voltage,
        HOUR(read_at) AS read_at_hour
    FROM metrics
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        kwh_dense_rank,
        kwh_percent_rank,
        site_id_avg_kwh,
        read_at_rank,
        kwh_rank_per_next3_voltage,
        read_at_hour,
        LEAD(next3_voltage) OVER (PARTITION BY kwh_dense_rank, quality ORDER BY kwh_percent_rank ASC, id) AS next_next3_voltage,
        ROUND(CUME_DIST() OVER w_kwh_dense_rank_quality_by_kw_2, 3) AS kwh_percent_rank_cume_dist,
        LAST_VALUE(site_id_avg_kwh) OVER (PARTITION BY kwh_dense_rank, quality ORDER BY kwh_percent_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_site_id_avg_kwh
    FROM enriched
    WINDOW w_kwh_dense_rank_quality_by_kw_2 AS (PARTITION BY kwh_dense_rank, quality ORDER BY kwh_percent_rank ASC)
)
SELECT
    id,
    site_id,
    kwh,
    kwh_dense_rank,
    kwh_percent_rank,
    site_id_avg_kwh,
    read_at_rank,
    kwh_rank_per_next3_voltage,
    read_at_hour,
    next_next3_voltage,
    kwh_percent_rank_cume_dist,
    last_site_id_avg_kwh,
    ROUND(kwh_dense_rank / NULLIF(last_site_id_avg_kwh, 0), 3) AS kwh_dense_rank_per_last_site_id_avg_kwh,
    LEAST(next_next3_voltage, voltage) AS least_next_next3_voltage_voltage
FROM metrics_2
WHERE kwh_percent_rank <= 0.8
ORDER BY next_next3_voltage DESC
