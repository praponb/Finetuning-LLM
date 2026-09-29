WITH recent_sites AS (
    -- Scope the sites rows
    SELECT *
    FROM sites
    WHERE zone IN ('Z3', 'Z4')
),
unique_rows AS (
    -- Keep the latest row per tariff
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY commissioned_on DESC, id DESC) AS recency_rank
        FROM recent_sites
    ) ranked
    WHERE recency_rank = 1
),
latest_per_key AS (
    -- Keep the latest row per tariff
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY installed_kw DESC, id DESC) AS rn
        FROM unique_rows
    ) ranked
    WHERE rn = 1
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        COUNT(*) AS reading_count_2,
        MIN(kwh) AS min_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        COALESCE(m.reading_count_2, 0) AS reading_count_2,
        m.min_reading_kwh
    FROM latest_per_key s
    LEFT JOIN reading_stats m ON m.site_id = s.id
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        ROUND(AVG(kwh), 2) AS avg_reading_kwh,
        MIN(read_at) AS first_reading_read_at,
        MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.avg_reading_kwh,
        m.first_reading_read_at,
        m.last_reading_read_at
    FROM profiled s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.id
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM summarized_rows s
    WHERE s.id IN (
            SELECT m.site_id
            FROM meter_readings m
        )
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT SUM(m.voltage) FROM meter_readings m WHERE m.site_id = s.id) AS total_reading_voltage
    FROM with_activity s
)
SELECT *
FROM (
    SELECT commissioned_on, zone, min_reading_kwh
    FROM with_counts
) src
PIVOT (
    MAX(min_reading_kwh)
    FOR zone IN ('Z4', 'Z2', 'Z3')
)
