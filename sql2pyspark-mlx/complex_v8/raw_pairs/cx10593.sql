WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE kwh < 106.0
      AND quality = 'BAD'
),
stats AS (
    -- Summarise per group
    SELECT
        quality_or_unknown,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        ROUND(SUM(kwh) / NULLIF(SUM(voltage), 0), 3) AS kwh_to_voltage_ratio,
        ROUND(100 * COUNT(CASE WHEN quality NOT IN ('BAD', 'EST') THEN 1 END) / COUNT(*), 1) AS pct_quality_not_in_bad_est,
        COUNT(*) AS cnt
    FROM recent
    GROUP BY quality_or_unknown, DATE_TRUNC('MONTH', read_at)
    HAVING AVG(voltage) > 215.7
),
ranked AS (
    -- Previous / next values per key
    SELECT
        quality_or_unknown,
        read_at_month,
        cnt,
        LAG(pct_quality_not_in_bad_est) OVER (PARTITION BY cnt ORDER BY kwh_to_voltage_ratio NULLS LAST, quality_or_unknown, read_at_month) AS prev_pct_quality_not_in_bad_est,
        NTILE(5) OVER (PARTITION BY cnt ORDER BY kwh_to_voltage_ratio NULLS LAST, quality_or_unknown, read_at_month) AS kwh_to_voltage_ratio_quintile
    FROM stats
)
SELECT
    quality_or_unknown,
    read_at_month,
    cnt,
    prev_pct_quality_not_in_bad_est,
    kwh_to_voltage_ratio_quintile,
    LOWER(REGEXP_REPLACE(TRIM(quality_or_unknown), ' ', '_')) AS quality_or_unknown_slug
FROM ranked
ORDER BY quality_or_unknown_slug DESC, quality_or_unknown, read_at_month
LIMIT 5
