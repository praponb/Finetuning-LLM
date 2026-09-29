WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage * 1.1, 2) AS voltage_adj,
        LENGTH(TRIM(quality)) AS quality_len,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
    FROM meter_readings
),
combined_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.voltage_adj,
        si.site_name
    FROM cleaned_readings s
    JOIN sites si ON si.id = s.site_id
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(voltage * 0.25, 2) AS voltage_adj_2,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label
    FROM combined_details
),
latest_per_key AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        voltage_adj,
        site_name,
        voltage_adj_2,
        days_since_read_at,
        read_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY read_at DESC, id DESC) AS recency_rank
        FROM annotated
    ) ranked
    WHERE recency_rank = 1
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('YEAR', read_at) AS read_at_year,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label_2,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at_2
    FROM latest_per_key
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, value
FROM (
    SELECT id, voltage, voltage_adj
    FROM scored
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (voltage, voltage_adj)
) unp
ORDER BY id, measure
