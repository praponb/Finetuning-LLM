WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        si.zone AS site_zone
    FROM recent_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(kwh) OVER (PARTITION BY site_id, quality ORDER BY kwh NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id, quality ORDER BY kwh DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_1_prior,
        voltage - LAG(voltage) OVER (PARTITION BY site_id, quality ORDER BY kwh NULLS LAST, id) AS voltage_change,
        NTILE(4) OVER (ORDER BY kwh DESC NULLS FIRST, id) AS kwh_quartile,
        MAX(voltage) OVER (PARTITION BY site_id, quality ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_voltage_so_far,
        NTILE(5) OVER (ORDER BY kwh DESC, id) AS kwh_quintile,
        COUNT(*) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at)) AS rows_in_site_id_read_at_month,
        DENSE_RANK() OVER w_site_id_quality_by_kwh_2 AS kwh_dense_rank,
        FIRST_VALUE(read_at) OVER (PARTITION BY site_id, quality ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        MAX(kwh) OVER (PARTITION BY quality ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS kwh_max_1_prior
    FROM enriched
    WINDOW w_site_id_quality_by_kwh_2 AS (PARTITION BY site_id, quality ORDER BY kwh DESC, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(voltage - 220.7) AS voltage_distance,
        COALESCE(max_voltage_so_far, 0.0) AS max_voltage_so_far_filled
    FROM sequenced
    WHERE kwh_avg_1_prior >= 48.0
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(voltage_change) OVER (PARTITION BY kwh_quartile) AS kwh_quartile_max_voltage_change,
        SIZE(COLLECT_SET(quality) OVER w_kwh_quartile) AS distinct_quality_in_kwh_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY quality, kwh_dense_rank ORDER BY read_at, id), 3) AS read_at_percent_rank,
        LEAD(kwh_avg_1_prior) OVER (ORDER BY kwh_avg_1_prior DESC, id) AS next_kwh_avg_1_prior,
        RANK() OVER (ORDER BY voltage_distance ASC, id) AS voltage_distance_rank,
        ROUND(max_voltage_so_far / NULLIF(SUM(max_voltage_so_far) OVER (PARTITION BY kwh_quartile), 0), 4) AS max_voltage_so_far_share_of_kwh_quartile,
        FIRST_VALUE(max_voltage_so_far) IGNORE NULLS OVER (ORDER BY kwh_avg_1_prior DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_voltage_so_far
    FROM annotated
    WINDOW w_kwh_quartile AS (PARTITION BY kwh_quartile)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        kwh_quartile,
        COUNT(*) AS row_count,
        SUM(kwh_share_of_site_id) AS total_kwh_share_of_site_id,
        MIN(voltage) AS min_voltage,
        ROUND(AVG(max_voltage_so_far), 2) AS avg_max_voltage_so_far
    FROM metrics
    WHERE kwh <= 80.8
    GROUP BY kwh_quartile
    HAVING MAX(kwh_share_of_site_id) > 0.1
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY kwh_quartile, row_count ORDER BY min_voltage ASC, kwh_quartile) AS rn,
        SUM(avg_max_voltage_so_far) OVER (PARTITION BY kwh_quartile, row_count ORDER BY min_voltage ASC, kwh_quartile ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS avg_max_voltage_so_far_sum_3_prior,
        MIN(min_voltage) OVER (PARTITION BY kwh_quartile, row_count) AS kwh_quartile_row_count_min_min_voltage
    FROM totals
)
SELECT
    kwh_quartile,
    row_count,
    total_kwh_share_of_site_id,
    min_voltage,
    avg_max_voltage_so_far,
    rn,
    avg_max_voltage_so_far_sum_3_prior,
    COALESCE(min_voltage, 0.0) AS min_voltage_filled,
    row_count - rn AS row_count_minus_rn
FROM with_windows
WHERE kwh_quartile < 9
ORDER BY min_voltage_filled, rn DESC, kwh_quartile
LIMIT 10
