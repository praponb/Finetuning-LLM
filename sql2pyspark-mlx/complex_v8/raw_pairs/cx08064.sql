WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage BETWEEN 220.2 AND 234.5
),
latest_per_key AS (
    -- Keep the latest row per quality, site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY voltage DESC, id DESC) AS recency_rank
        FROM cleaned_readings
    ) ranked
    WHERE recency_rank = 1
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM latest_per_key s
    JOIN meter_readings m ON m.site_id = s.site_id
),
annotated AS (
    -- Add calculated columns
    SELECT *, DATE_TRUNC('MONTH', read_at) AS read_at_month
    FROM line_level_readings
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(kwh) OVER (PARTITION BY quality, reading_id ORDER BY reading_kwh, id, reading_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS kwh_sum_1_prior,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id, reading_id ORDER BY kwh ASC, id, reading_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_2_prior,
        LAG(reading_kwh) OVER (PARTITION BY site_id, reading_id ORDER BY kwh ASC, id, reading_id) AS prev_reading_kwh,
        NTH_VALUE(reading_kwh, 2) OVER (PARTITION BY site_id, reading_id ORDER BY kwh ASC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reading_kwh,
        ROUND(reading_voltage / NULLIF(SUM(reading_voltage) OVER (PARTITION BY site_id, reading_id), 0), 4) AS reading_voltage_share_of_site_id_reading_id
    FROM annotated
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    reading_id,
    reading_voltage,
    reading_kwh,
    read_at_month,
    kwh_sum_1_prior,
    voltage_avg_2_prior,
    prev_reading_kwh,
    second_reading_kwh,
    reading_voltage_share_of_site_id_reading_id
FROM with_trends
ORDER BY read_at, id, reading_id
LIMIT 10
