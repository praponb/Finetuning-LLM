WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(voltage) OVER (PARTITION BY quality ORDER BY voltage DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS voltage_max_1_prior,
        DENSE_RANK() OVER (ORDER BY read_at ASC NULLS LAST, id) AS read_at_dense_rank,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        RANK() OVER (PARTITION BY quality ORDER BY voltage DESC) AS voltage_rank
    FROM cleaned
    WINDOW w_site_id AS (PARTITION BY site_id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER w_read_at_dense_rank_site_id_b AS voltage_dense_rank,
        MAX(voltage_rank) OVER (PARTITION BY read_at_dense_rank, site_id ORDER BY voltage ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS voltage_rank_max_4,
        RANK() OVER (PARTITION BY read_at_dense_rank, site_id ORDER BY voltage ASC, id) AS voltage_rank_2,
        MAX(kwh) OVER (PARTITION BY read_at_dense_rank, site_id ORDER BY voltage ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS kwh_max_7,
        LAST_VALUE(rows_in_site_id) OVER (PARTITION BY read_at_dense_rank, site_id ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rows_in_site_id
    FROM with_windows
    WINDOW w_read_at_dense_rank_site_id_b AS (PARTITION BY read_at_dense_rank, site_id ORDER BY voltage ASC)
)
SELECT *
FROM (
    SELECT voltage_dense_rank, quality, voltage
    FROM with_trends
) src
PIVOT (
    COUNT(voltage) AS count_v, SUM(voltage) AS sum_v
    FOR quality IN ('BAD', 'EST', 'OK')
)
ORDER BY voltage_dense_rank
