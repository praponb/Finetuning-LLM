WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE kwh < 86.0
      AND voltage <= 239.6
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        voltage,
        FIRST_VALUE(kwh) OVER (PARTITION BY site_id ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id) AS rows_in_read_at_month_site_id,
        voltage - LAG(voltage) OVER (PARTITION BY site_id ORDER BY read_at ASC, id) AS voltage_change
    FROM filtered
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_read_at_month AS rows_in_read_at_month,
        MAX(voltage) OVER (PARTITION BY MONTH(read_at) ORDER BY voltage_change ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS voltage_max_6_prior,
        COUNT(*) OVER (PARTITION BY MONTH(read_at) ORDER BY voltage_change ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(voltage_change) OVER (PARTITION BY MONTH(read_at)) AS read_at_month_sum_voltage_change,
        ROUND(AVG(first_kwh) OVER (ORDER BY voltage_change DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS first_kwh_avg_3_prior,
        DENSE_RANK() OVER (PARTITION BY MONTH(read_at) ORDER BY voltage_change ASC) AS voltage_change_dense_rank
    FROM windowed
    WINDOW w_read_at_month AS (PARTITION BY MONTH(read_at))
),
enriched AS (
    -- Add calculated columns
    SELECT *, DATE_TRUNC('WEEK', read_at) AS read_at_week
    FROM sequenced
)
SELECT
    id,
    read_at,
    voltage,
    first_kwh,
    last_known_read_at,
    rows_in_read_at_month_site_id,
    voltage_change,
    rows_in_read_at_month,
    running_count,
    read_at_month_sum_voltage_change,
    first_kwh_avg_3_prior,
    voltage_change_dense_rank,
    read_at_week,
    DATE_TRUNC('WEEK', last_known_read_at) AS last_known_read_at_week,
    DATE_TRUNC('DAY', last_known_read_at) AS last_known_read_at_day
FROM enriched
ORDER BY voltage, first_kwh_avg_3_prior, id
LIMIT 20
