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
        GREATEST(voltage, kwh) AS greatest_voltage_kwh,
        voltage - kwh AS voltage_minus_kwh
    FROM meter_readings
    WHERE site_id >= 3
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        kwh,
        quality,
        quality_ref,
        greatest_voltage_kwh,
        voltage_minus_kwh,
        CONCAT_WS('|', quality_ref, quality) AS quality_ref_quality_label,
        MONTH(read_at) AS read_at_month,
        INITCAP(TRIM(quality_ref)) AS quality_ref_clean
    FROM valid
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        greatest_voltage_kwh,
        read_at_month,
        quality_ref_clean,
        LENGTH(TRIM(quality_ref)) AS quality_ref_len,
        ROUND(greatest_voltage_kwh * 1.5, 2) AS greatest_voltage_kwh_adj,
        ROUND(greatest_voltage_kwh / NULLIF(voltage_minus_kwh, 0), 3) AS greatest_voltage_kwh_per_voltage_minus_kwh
    FROM tagged
    WHERE voltage_minus_kwh >= 54.8
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        greatest_voltage_kwh,
        read_at_month,
        quality_ref_clean,
        quality_ref_len,
        greatest_voltage_kwh_per_voltage_minus_kwh,
        FIRST_VALUE(greatest_voltage_kwh) IGNORE NULLS OVER (PARTITION BY read_at_month ORDER BY greatest_voltage_kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_greatest_voltage_kwh,
        MAX(read_at_month) OVER w_read_at_month_quality_ref_le AS read_at_month_quality_ref_len_max_read_at_month
    FROM enriched
    WINDOW w_read_at_month_quality_ref_le AS (PARTITION BY read_at_month, quality_ref_len)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM metrics
    WHERE read_at_month > (SELECT AVG(read_at_month) FROM metrics)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at_month,
        quality_ref_clean,
        greatest_voltage_kwh_per_voltage_minus_kwh,
        read_at_month_quality_ref_len_max_read_at_month,
        SUM(greatest_voltage_kwh) OVER w_quality_ref_len_read_at_mont AS quality_ref_len_read_at_month_sum_greatest_volta,
        ROUND(AVG(quality_ref_len) OVER (PARTITION BY quality_ref_len, read_at_month), 2) AS quality_ref_len_read_at_month_avg_quality_ref_le,
        ROUND(quality_ref_len / NULLIF(SUM(quality_ref_len) OVER (PARTITION BY quality_ref_len, read_at_month), 0), 4) AS quality_ref_len_share_of_quality_ref_len_read_at,
        ROUND(PERCENT_RANK() OVER w_read_at_month_quality_ref_le_2, 3) AS read_at_month_percent_rank,
        read_at_month - LAG(read_at_month) OVER (PARTITION BY read_at_month, quality_ref_len ORDER BY read_at_month NULLS LAST, id) AS read_at_month_change,
        SUM(quality_ref_len) OVER (PARTITION BY quality_ref_len, read_at_month) AS quality_ref_len_read_at_month_sum_quality_ref_le
    FROM filtered
    WINDOW w_quality_ref_len_read_at_mont AS (PARTITION BY quality_ref_len, read_at_month), w_read_at_month_quality_ref_le_2 AS (PARTITION BY read_at_month, quality_ref_len ORDER BY read_at_month NULLS LAST, id)
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM with_trends
    WHERE greatest_voltage_kwh_per_voltage_minus_kwh < (SELECT MAX(greatest_voltage_kwh_per_voltage_minus_kwh) FROM with_trends)
)
-- Final projection
SELECT
    id,
    read_at_month,
    greatest_voltage_kwh_per_voltage_minus_kwh,
    read_at_month_quality_ref_len_max_read_at_month,
    quality_ref_len_read_at_month_sum_greatest_volta,
    quality_ref_len_share_of_quality_ref_len_read_at,
    read_at_month_percent_rank,
    read_at_month_change,
    quality_ref_len_read_at_month_sum_quality_ref_le,
    LOWER(REGEXP_REPLACE(TRIM(quality_ref_clean), ' ', '_')) AS quality_ref_clean_slug,
    read_at_month_change - read_at_month AS read_at_month_change_minus_read_at_month
FROM with_activity
ORDER BY read_at_month_change
