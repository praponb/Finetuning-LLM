WITH prepared_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        read_at + INTERVAL 7 DAYS AS read_at_plus_7d
    FROM meter_readings
    WHERE quality = 'BAD'
      AND site_id <= 9
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        read_at_plus_7d,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_site_id, 0), 4) AS voltage_share_of_site_id,
        NTH_VALUE(read_at_plus_7d, 2) OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at_plus_7d,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY TO_DATE(read_at_plus_7d) ORDER BY voltage DESC, id))) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(read_at_plus_7d) ORDER BY voltage DESC), 3) AS voltage_cume_dist
    FROM prepared_readings
    WINDOW w_site_id AS (PARTITION BY site_id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        read_at,
        read_at_plus_7d,
        voltage_share_of_site_id,
        second_read_at_plus_7d,
        minutes_since_prev_read_at,
        voltage_cume_dist,
        DATE_TRUNC('YEAR', read_at) AS read_at_year,
        kwh + voltage_cume_dist AS kwh_plus_voltage_cume_dist,
        ROUND(voltage_cume_dist / NULLIF(voltage, 0), 3) AS voltage_cume_dist_per_voltage
    FROM sequenced
    WHERE voltage_cume_dist BETWEEN 0.2 AND 0.9
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE minutes_since_prev_read_at > (SELECT AVG(minutes_since_prev_read_at) FROM tagged)
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM with_activity
    WHERE minutes_since_prev_read_at < (SELECT MAX(minutes_since_prev_read_at) FROM with_activity)
)
SELECT
    id,
    read_at,
    read_at_plus_7d,
    voltage_share_of_site_id,
    second_read_at_plus_7d,
    minutes_since_prev_read_at,
    voltage_cume_dist,
    read_at_year,
    kwh_plus_voltage_cume_dist,
    voltage_cume_dist_per_voltage,
    GREATEST(voltage_share_of_site_id, voltage_cume_dist) AS greatest_voltage_share_of_site_id_voltage_cume_d
FROM screened
WHERE minutes_since_prev_read_at < 3000.0
ORDER BY kwh_plus_voltage_cume_dist
