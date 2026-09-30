WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        voltage - kwh AS voltage_minus_kwh
    FROM meter_readings
    WHERE quality = 'EST'
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM filtered_readings s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE m.quality IS NOT NULL
        )
),
tagged AS (
    -- Add calculated columns
    SELECT *, CONCAT_WS('-', quality, quality_ref) AS quality_quality_ref_label
    FROM screened
    WHERE voltage > 216.1
),
grouped AS (
    -- Summarise per group
    SELECT
        TO_DATE(read_at) AS read_at_day,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        COUNT(*) AS n_rows,
        SUM(voltage_minus_kwh) FILTER (WHERE quality IN ('EST', 'BAD')) AS voltage_minus_kwh_quality_in_est_bad,
        COUNT(DISTINCT site_id) AS distinct_site_ids
    FROM tagged
    GROUP BY TO_DATE(read_at)
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM grouped
    WHERE voltage_minus_kwh_quality_in_est_bad > (SELECT MIN(voltage_minus_kwh_quality_in_est_bad) FROM grouped)
),
scored AS (
    -- Add calculated columns
    SELECT
        read_at_day,
        n_rows,
        voltage_minus_kwh_quality_in_est_bad,
        distinct_site_ids,
        CASE WHEN distinct_site_ids >= 8 THEN 'large' WHEN distinct_site_ids >= 5 THEN 'medium' ELSE 'small' END AS distinct_site_ids_tier,
        QUARTER(read_at_day) AS read_at_day_quarter,
        DATEDIFF(DATE '2025-12-31', read_at_day) AS days_since_read_at_day
    FROM with_activity
    WHERE n_rows BETWEEN 4 AND 10
),
distinct_site_ids_tier_stats AS (
    -- Max read_at_day_quarter per distinct_site_ids_tier
    SELECT distinct_site_ids_tier, MAX(read_at_day_quarter) AS max_read_at_day_quarter
    FROM scored
    GROUP BY distinct_site_ids_tier
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_read_at_day_quarter,
        ROUND(s.read_at_day_quarter - g.max_read_at_day_quarter, 2) AS read_at_day_quarter_minus_max
    FROM scored s
    JOIN distinct_site_ids_tier_stats g ON g.distinct_site_ids_tier = s.distinct_site_ids_tier
    WHERE s.read_at_day_quarter = g.max_read_at_day_quarter
)
-- Native SQL UNPIVOT (wide to long)
SELECT read_at_day, measure, value
FROM (
    SELECT read_at_day, voltage_minus_kwh_quality_in_est_bad, max_read_at_day_quarter
    FROM vs_group
) src
UNPIVOT (
    value FOR measure IN (voltage_minus_kwh_quality_in_est_bad AS voltage_minus_kwh_quality_in_est_bad_v, max_read_at_day_quarter AS max_read_at_day_quarter_v)
) unp
ORDER BY read_at_day, measure
