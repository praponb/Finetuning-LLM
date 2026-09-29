WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-25 11:00:00'
      AND voltage <= 240.9
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM cleaned
    WHERE kwh > (SELECT AVG(kwh) FROM cleaned)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY quality ORDER BY days_since_read_at DESC) AS days_since_read_at_rank,
        SUM(kwh) OVER (PARTITION BY quality ORDER BY days_since_read_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS kwh_sum_2_prior,
        LAST_VALUE(voltage) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at) ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage,
        SUM(voltage) OVER (PARTITION BY site_id) AS site_id_sum_voltage,
        ROUND(days_since_read_at / NULLIF(SUM(days_since_read_at) OVER w_site_id, 0), 4) AS days_since_read_at_share_of_site_id,
        SUM(days_since_read_at) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at) ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_read_at
    FROM with_activity
    WINDOW w_site_id AS (PARTITION BY site_id)
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_ref,
        s.days_since_read_at,
        s.days_since_read_at_rank,
        s.kwh_sum_2_prior,
        s.last_voltage,
        s.site_id_sum_voltage,
        s.days_since_read_at_share_of_site_id,
        s.running_days_since_read_at,
        si.zone AS site_zone
    FROM ranked s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    site_zone,
    days_since_read_at_rank,
    GROUPING_ID() AS grouping_level,
    MAX(read_at) AS last_read_at
FROM enriched
GROUP BY GROUPING SETS ((site_zone, days_since_read_at_rank), (site_zone))
ORDER BY site_zone NULLS LAST, days_since_read_at_rank NULLS LAST
