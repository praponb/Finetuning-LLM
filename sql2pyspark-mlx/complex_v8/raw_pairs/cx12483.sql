WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE site_id <= 5
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        quality,
        read_at_date,
        DATEDIFF(read_at_date, LAG(read_at_date) OVER (PARTITION BY quality ORDER BY read_at, id)) AS days_since_prev_read_at_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY read_at ASC), 3) AS read_at_percent_rank,
        LAG(read_at, 2) OVER (PARTITION BY site_id ORDER BY kwh DESC, id) AS prev2_read_at,
        DENSE_RANK() OVER w_quality_by_read_at_ties AS read_at_dense_rank,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY quality ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage,
        ROW_NUMBER() OVER w_quality_by_kwh AS rn,
        DENSE_RANK() OVER w_site_id_by_kwh AS kwh_dense_rank,
        SUM(kwh) OVER (PARTITION BY site_id ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh,
        FIRST_VALUE(read_at) OVER (PARTITION BY quality ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at,
        LAG(read_at_date, 3) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev3_read_at_date,
        LEAD(read_at, 2) OVER (PARTITION BY site_id ORDER BY kwh DESC, id) AS next2_read_at,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        NTILE(3) OVER w_quality_by_kwh AS kwh_tercile,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id), 2) AS voltage_vs_site_id_avg
    FROM filtered_readings
    WINDOW w_quality_by_read_at_ties AS (PARTITION BY quality ORDER BY read_at), w_quality_by_kwh AS (PARTITION BY quality ORDER BY kwh ASC, id), w_site_id_by_kwh AS (PARTITION BY site_id ORDER BY kwh DESC, id)
),
grouped AS (
    -- Group-level totals
    SELECT
        CASE WHEN read_at_percent_rank >= 0.8 THEN 'HIGH' WHEN read_at_percent_rank >= 0.4 THEN 'MEDIUM' ELSE 'LOW' END AS read_at_percent_rank_tier,
        YEAR(read_at) AS read_at_year,
        YEAR(read_at_date) AS read_at_date_year,
        PERCENTILE_APPROX(first_known_voltage, 0.5) AS median_first_known_voltage,
        ROUND(100 * COUNT(CASE WHEN quality = 'BAD' THEN 1 END) / COUNT(*), 1) AS pct_quality_bad,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        MAX(running_kwh) AS max_running_kwh,
        COUNT(DISTINCT id) AS distinct_ids
    FROM windowed
    GROUP BY CASE WHEN read_at_percent_rank >= 0.8 THEN 'HIGH' WHEN read_at_percent_rank >= 0.4 THEN 'MEDIUM' ELSE 'LOW' END, YEAR(read_at), YEAR(read_at_date)
)
-- Final projection
SELECT
    read_at_percent_rank_tier,
    read_at_year,
    read_at_date_year,
    median_first_known_voltage,
    pct_quality_bad,
    n_rows,
    quality_list,
    max_running_kwh
FROM grouped
ORDER BY pct_quality_bad
