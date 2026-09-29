WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 1.1, 2) AS kwh_adj,
        ROUND(kwh * 1.5, 2) AS kwh_adj_2
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 10 AND 15
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        kwh + voltage AS kwh_plus_voltage,
        ROUND(kwh_adj / NULLIF(kwh_adj_2, 0), 3) AS kwh_adj_per_kwh_adj_2,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
    FROM valid_readings
    WHERE read_at < TIMESTAMP '2025-02-24 11:00:00'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(kwh_adj_per_kwh_adj_2, 2) OVER (PARTITION BY TO_DATE(read_at) ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh_adj_per_kwh_adj_2,
        MAX(kwh) OVER (ORDER BY kwh_adj_2 ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far,
        LEAD(kwh_adj_per_kwh_adj_2, 3) OVER (ORDER BY kwh_adj_2 ASC NULLS LAST, id) AS next3_kwh_adj_per_kwh_adj_2,
        SUM(kwh_adj_per_kwh_adj_2) OVER (PARTITION BY site_id, quality ORDER BY kwh DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS kwh_adj_per_kwh_adj_2_sum_6_prior,
        ROW_NUMBER() OVER (ORDER BY kwh_adj_2 ASC NULLS LAST, id) AS rn
    FROM enriched
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
        kwh_adj,
        kwh_adj_2,
        kwh_plus_voltage,
        kwh_adj_per_kwh_adj_2,
        quality_prefix,
        second_kwh_adj_per_kwh_adj_2,
        max_kwh_so_far,
        next3_kwh_adj_per_kwh_adj_2,
        kwh_adj_per_kwh_adj_2_sum_6_prior
    FROM ranked
    WHERE rn <= 3
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT site_id, read_at, quality, kwh
    FROM first_rows
) src
PIVOT (
    AVG(kwh) AS avg_v, SUM(kwh) AS sum_v
    FOR quality IN ('OK', 'BAD')
)
ORDER BY site_id, read_at
