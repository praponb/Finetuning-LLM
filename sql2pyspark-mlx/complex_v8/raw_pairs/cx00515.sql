WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        TO_DATE(read_at) AS read_at_date,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-25 09:00:00'
      AND voltage > 215.8
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        quality,
        read_at_date,
        quality_ref,
        SUM(kwh) OVER w_read_at_year_quality AS read_at_year_quality_sum_kwh,
        MAX(kwh) OVER (PARTITION BY site_id, TO_DATE(read_at) ORDER BY kwh ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS kwh_max_2_prior
    FROM base_readings
    WINDOW w_read_at_year_quality AS (PARTITION BY YEAR(read_at), quality)
)
SELECT *
FROM (
    SELECT read_at, quality, read_at_year_quality_sum_kwh
    FROM with_trends
) src
PIVOT (
    AVG(read_at_year_quality_sum_kwh)
    FOR quality IN ('EST', 'OK', 'BAD')
)
ORDER BY read_at
