WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        CASE WHEN voltage >= 231.6 THEN 'A' WHEN voltage >= 223.1 THEN 'B' ELSE 'C' END AS voltage_tier
    FROM meter_readings
    WHERE voltage <= 239.6
      AND quality <> 'BAD'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(kwh) OVER (PARTITION BY quality) AS quality_sum_kwh,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        ROUND(PERCENT_RANK() OVER w_all_by_kwh, 3) AS kwh_percent_rank,
        DENSE_RANK() OVER w_quality_by_kwh_ties AS kwh_dense_rank,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_quality_by_kwh)) / 60, 1) AS minutes_since_prev_read_at,
        NTILE(4) OVER w_voltage_tier_by_kwh AS kwh_quartile,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        LAG(read_at) OVER (PARTITION BY quality ORDER BY kwh, id) AS prev_read_at,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY quality ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        ROW_NUMBER() OVER (ORDER BY kwh DESC, id) AS rn
    FROM cleaned
    WINDOW w_all_by_kwh AS (ORDER BY kwh DESC, id), w_quality_by_kwh_ties AS (PARTITION BY quality ORDER BY kwh), w_quality_by_kwh AS (PARTITION BY quality ORDER BY kwh, id), w_voltage_tier_by_kwh AS (PARTITION BY voltage_tier ORDER BY kwh DESC NULLS FIRST, id)
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_ref,
        voltage_tier,
        quality_sum_kwh,
        first_known_kwh,
        kwh_percent_rank,
        minutes_since_prev_read_at,
        kwh_quartile,
        rows_in_quality,
        prev_read_at,
        second_kwh,
        rn
    FROM with_windows
    WHERE kwh_dense_rank <= 3
)
SELECT
    id,
    site_id,
    read_at,
    voltage_tier,
    quality_sum_kwh,
    first_known_kwh,
    kwh_percent_rank,
    minutes_since_prev_read_at,
    rows_in_quality,
    prev_read_at,
    rn
FROM first_rows
