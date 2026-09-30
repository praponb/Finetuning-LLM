WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        CASE WHEN voltage >= 235.0 THEN 'large' WHEN voltage >= 223.3 THEN 'medium' ELSE 'small' END AS voltage_tier
    FROM meter_readings
    WHERE voltage > 221.8
      AND MONTH(read_at) IN (1, 2)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(kwh) OVER (PARTITION BY quality) AS quality_max_kwh,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh
    FROM valid
),
latest AS (
    -- Keep the latest row per quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_ref,
        voltage_tier,
        quality_max_kwh,
        first_known_kwh
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY voltage DESC, id DESC) AS row_num
        FROM ranked
    ) ranked
    WHERE row_num = 1
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at_2
    FROM latest s
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS max_reading_kwh,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM profiled s
),
deduped AS (
    -- Keep the latest row per quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_ref,
        voltage_tier,
        quality_max_kwh,
        first_known_kwh,
        first_reading_read_at,
        first_reading_read_at_2,
        max_reading_kwh,
        last_reading_read_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY first_known_kwh DESC, id DESC) AS recency_rank
        FROM summarized_rows
    ) ranked
    WHERE recency_rank = 1
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT s.*, si.site_name, si.commissioned_on AS site_commissioned_on
    FROM deduped s
    JOIN sites si ON si.id = s.site_id
    WHERE si.site_name IS NOT NULL
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at, quality, quality_max_kwh
    FROM enriched_site
) src
PIVOT (
    MAX(quality_max_kwh)
    FOR quality IN ('EST', 'BAD')
)
