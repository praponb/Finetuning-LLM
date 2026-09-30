WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        LOWER(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE quality IN ('EST', 'OK')
      AND read_at < TIMESTAMP '2025-02-18 10:00:00'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ROUND(AVG(voltage) OVER (PARTITION BY MONTH(read_at) ORDER BY kwh ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_3_prior,
        MAX(voltage) OVER (PARTITION BY site_id ORDER BY read_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS voltage_max_4,
        SUM(kwh) OVER w_read_at_year AS read_at_year_sum_kwh,
        DENSE_RANK() OVER (PARTITION BY site_id ORDER BY read_at DESC) AS read_at_dense_rank
    FROM base_readings
    WINDOW w_read_at_year AS (PARTITION BY YEAR(read_at))
),
latest_per_key AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        voltage_avg_3_prior,
        voltage_max_4,
        read_at_year_sum_kwh,
        read_at_dense_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh DESC, id DESC) AS rn
        FROM with_trends
    ) ranked
    WHERE rn = 1
),
top_per_group AS (
    -- Top 2 per group
    SELECT *
    FROM latest_per_key
    WHERE read_at_dense_rank <= 2
)
SELECT id, metric, amount
FROM (
    SELECT id, voltage, kwh
    FROM top_per_group
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric IN (voltage, kwh)
) unp
