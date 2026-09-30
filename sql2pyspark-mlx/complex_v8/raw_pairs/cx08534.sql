WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-31 10:00:00'
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        kwh,
        quality,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM base_readings
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(kwh) OVER (PARTITION BY quality_group, quality ORDER BY kwh DESC, id) AS prev_kwh,
        SUM(kwh) OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh,
        MIN(kwh) OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_so_far,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY quality_group, quality ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        ROW_NUMBER() OVER (PARTITION BY quality_group, quality ORDER BY kwh DESC, id) AS kwh_seq
    FROM annotated
)
SELECT
    id,
    kwh,
    quality,
    running_kwh,
    min_kwh_so_far,
    first_known_kwh,
    kwh_seq,
    INITCAP(TRIM(quality)) AS quality_clean
FROM sequenced
WHERE kwh_seq >= 3
ORDER BY running_kwh, kwh DESC
