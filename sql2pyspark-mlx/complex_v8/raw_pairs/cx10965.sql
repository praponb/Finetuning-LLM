WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE quality = 'EST'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(days_since_read_at) OVER (PARTITION BY site_id, quality) AS site_id_quality_min_days_since_read_at,
        ROUND(CUME_DIST() OVER (ORDER BY voltage DESC, id), 3) AS voltage_cume_dist,
        ROUND(AVG(days_since_read_at) OVER (PARTITION BY site_id, TO_DATE(read_at) ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS days_since_read_at_avg_3_prior,
        DENSE_RANK() OVER (ORDER BY voltage DESC, id) AS voltage_dense_rank,
        MAX(days_since_read_at) OVER (ORDER BY kwh DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS days_since_read_at_max_3_prior,
        LAST_VALUE(voltage) OVER (PARTITION BY site_id, quality ORDER BY days_since_read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage,
        FIRST_VALUE(read_at) OVER (PARTITION BY site_id, TO_DATE(read_at) ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        ROW_NUMBER() OVER w_quality_by_voltage AS voltage_seq,
        LAST_VALUE(voltage) OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage_2,
        MAX(days_since_read_at) OVER (PARTITION BY site_id, TO_DATE(read_at) ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_days_since_read_at_so_far,
        NTILE(5) OVER (ORDER BY voltage DESC, id) AS voltage_quintile,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY quality ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at
    FROM valid
    WINDOW w_site_id AS (PARTITION BY site_id), w_quality_by_voltage AS (PARTITION BY quality ORDER BY voltage DESC, id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        days_since_read_at,
        site_id_quality_min_days_since_read_at,
        days_since_read_at_avg_3_prior,
        days_since_read_at_max_3_prior,
        last_voltage,
        first_read_at,
        voltage_seq,
        last_voltage_2,
        max_days_since_read_at_so_far,
        voltage_quintile,
        DATEDIFF(DATE '2026-01-01', first_read_at) AS days_since_first_read_at,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        CASE WHEN quality IN ('EST', 'OK') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM sequenced
    WHERE rows_in_site_id <= 17
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(first_read_at) AS first_read_at_year,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        voltage_seq,
        ROUND(SUM(days_since_first_read_at) / NULLIF(SUM(last_voltage_2), 0), 3) AS days_since_first_read_at_to_last_voltage_2_ratio,
        ROUND(AVG(last_voltage_2), 2) AS avg_last_voltage_2
    FROM labeled
    GROUP BY YEAR(first_read_at), DATE_TRUNC('MONTH', read_at), voltage_seq
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        first_read_at_year,
        read_at_month,
        voltage_seq,
        days_since_first_read_at_to_last_voltage_2_ratio,
        avg_last_voltage_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY voltage_seq ORDER BY days_since_first_read_at_to_last_voltage_2_ratio DESC, first_read_at_year, read_at_month, voltage_seq), 3) AS days_since_first_read_at_to_last_voltage_2_ratio_2,
        MIN(first_read_at_year) OVER w_first_read_at_year AS first_read_at_year_min_first_read_at_year
    FROM grouped
    WINDOW w_first_read_at_year AS (PARTITION BY first_read_at_year)
)
-- Final projection
SELECT
    first_read_at_year,
    read_at_month,
    voltage_seq,
    days_since_first_read_at_to_last_voltage_2_ratio,
    avg_last_voltage_2,
    days_since_first_read_at_to_last_voltage_2_ratio_2,
    first_read_at_year_min_first_read_at_year,
    ABS(avg_last_voltage_2 - 231.0) AS avg_last_voltage_2_distance,
    CASE WHEN avg_last_voltage_2 >= 231.0 THEN 'large' WHEN avg_last_voltage_2 >= 224.0 THEN 'medium' ELSE 'small' END AS avg_last_voltage_2_tier
FROM ranked
ORDER BY days_since_first_read_at_to_last_voltage_2_ratio_2, read_at_month NULLS LAST, first_read_at_year, voltage_seq
LIMIT 5
