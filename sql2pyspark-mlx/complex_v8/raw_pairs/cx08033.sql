WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        TO_DATE(read_at) AS read_at_date,
        kwh - voltage AS kwh_minus_voltage,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM valid
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        read_at_date,
        kwh_per_voltage,
        DAYOFWEEK(read_at) AS read_at_dayofweek
    FROM derived
    WHERE kwh BETWEEN 29.2 AND 106.0
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT MIN(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS min_reading_kwh,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at_2
    FROM annotated s
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM profiled
    WHERE kwh_per_voltage > (SELECT AVG(kwh_per_voltage) FROM profiled)
),
joined AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.read_at_date,
        s.kwh_per_voltage,
        s.read_at_dayofweek,
        s.min_reading_kwh,
        s.last_reading_read_at_2,
        si.site_name
    FROM without_matches s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    read_at_date,
    kwh_per_voltage,
    read_at_dayofweek,
    site_name,
    DATEDIFF(DATE '2026-01-01', read_at_date) AS days_since_read_at_date
FROM joined
ORDER BY site_id DESC, kwh NULLS LAST
