WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE voltage < 232.7
),
unique_rows AS (
    -- Keep the latest row per quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_ref,
        voltage_per_kwh,
        quality_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY voltage DESC, id DESC) AS row_num
        FROM valid_readings
    ) ranked
    WHERE row_num = 1
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT SUM(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_voltage
    FROM unique_rows s
),
site_id_stats AS (
    -- Median quality_len per site_id
    SELECT site_id, PERCENTILE_APPROX(quality_len, 0.5) AS median_quality_len
    FROM with_stats
    GROUP BY site_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_quality_len,
        ROUND(s.quality_len - g.median_quality_len, 2) AS quality_len_minus_median
    FROM with_stats s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.quality_len >= g.median_quality_len
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, voltage, voltage_per_kwh, kwh, quality_len_minus_median
    FROM compared
) src
UNPIVOT (
    amount FOR measure IN (voltage AS m_voltage, voltage_per_kwh AS voltage_per_kwh_v, kwh AS kwh_v, quality_len_minus_median AS quality_len_minus_median_v)
) unp
ORDER BY id, measure
