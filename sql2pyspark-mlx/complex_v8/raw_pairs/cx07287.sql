WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        CASE WHEN quality IN ('OK', 'BAD') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE voltage >= 213.7
),
latest AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        voltage_bucket,
        quality_slug,
        quality_group
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY read_at DESC, id DESC) AS recency_rank
        FROM valid
    ) ranked
    WHERE recency_rank = 1
),
latest_2 AS (
    -- Keep the latest row per quality
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        voltage_bucket,
        quality_slug,
        quality_group
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY voltage_bucket DESC, id DESC) AS rn
        FROM latest
    ) ranked
    WHERE rn = 1
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        quality_group,
        SUM(CASE WHEN quality = 'OK' THEN voltage ELSE 0 END) AS voltage_quality_ok,
        SUM(CASE WHEN quality <> 'BAD' THEN voltage ELSE 0 END) AS voltage_quality_bad,
        SUM(CASE WHEN quality = 'OK' THEN voltage_bucket ELSE 0 END) AS voltage_bucket_quality_ok,
        MAX(voltage_bucket) AS max_voltage_bucket
    FROM latest_2
    GROUP BY 1
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(quality_group)) AS quality_group_len,
        UPPER(TRIM(quality_group)) AS quality_group_clean,
        COALESCE(voltage_quality_bad, 0.0) AS voltage_quality_bad_filled
    FROM summary
    WHERE max_voltage_bucket < 242
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM tagged
    WHERE voltage_bucket_quality_ok > (SELECT MIN(voltage_bucket_quality_ok) FROM tagged)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(quality_group_len / 2) * 2 AS quality_group_len_bucket,
        voltage_quality_bad - voltage_quality_ok AS voltage_quality_bad_minus_voltage_quality_ok,
        FLOOR(max_voltage_bucket / 5) * 5 AS max_voltage_bucket_bucket
    FROM qualified
    WHERE max_voltage_bucket > 222
)
-- Final projection
SELECT
    quality_group,
    voltage_quality_bad,
    voltage_bucket_quality_ok,
    max_voltage_bucket,
    quality_group_len,
    quality_group_len_bucket,
    voltage_quality_bad_minus_voltage_quality_ok,
    max_voltage_bucket_bucket,
    FLOOR(max_voltage_bucket / 5) * 5 AS max_voltage_bucket_bucket_2,
    LEAST(max_voltage_bucket, quality_group_len_bucket) AS least_max_voltage_bucket_quality_group_len_bucke
FROM derived
ORDER BY voltage_quality_bad, quality_group
LIMIT 20
