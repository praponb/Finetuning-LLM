WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage >= 224.8
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        MAX(kwh) OVER (PARTITION BY YEAR(read_at), quality ORDER BY read_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS kwh_max_2_prior,
        SUM(kwh) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh,
        DENSE_RANK() OVER w_all_by_kwh_ties AS kwh_dense_rank,
        MAX(kwh) OVER (PARTITION BY YEAR(read_at), quality ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far,
        MAX(kwh) OVER (PARTITION BY TO_DATE(read_at)) AS read_at_to_date_max_kwh,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY site_id ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        LAST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh,
        LAG(kwh, 3) OVER (ORDER BY read_at DESC, id) AS prev3_kwh,
        LEAD(voltage, 1, 0) OVER (ORDER BY kwh DESC, id) AS next_voltage,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY TO_DATE(read_at))) AS distinct_quality_in_read_at_to_date,
        LAG(read_at, 2) OVER (ORDER BY kwh DESC, id) AS prev2_read_at,
        ROUND(AVG(kwh) OVER (ORDER BY read_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_3_prior
    FROM filtered
    WINDOW w_all_by_kwh_ties AS (ORDER BY kwh DESC)
)
SELECT
    id,
    site_id,
    read_at,
    voltage,
    kwh_max_2_prior,
    kwh_dense_rank,
    max_kwh_so_far,
    read_at_to_date_max_kwh,
    second_read_at,
    last_known_kwh,
    next_voltage,
    distinct_quality_in_read_at_to_date,
    prev2_read_at,
    kwh_avg_3_prior
FROM ranked
WHERE kwh_dense_rank <= 7
ORDER BY max_kwh_so_far
