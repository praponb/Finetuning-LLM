WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE quality <> 'BAD'
      AND read_at >= TIMESTAMP '2025-01-25 09:00:00'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        voltage,
        quality,
        SUM(days_since_read_at) OVER (PARTITION BY site_id) AS site_id_sum_days_since_read_at,
        LEAD(days_since_read_at) OVER (PARTITION BY site_id ORDER BY read_at DESC, id) AS next_days_since_read_at,
        LAST_VALUE(voltage) OVER (PARTITION BY site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage,
        ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY read_at DESC, id) AS rn,
        MIN(days_since_read_at) OVER w_site_id AS site_id_min_days_since_read_at
    FROM valid
    WINDOW w_site_id AS (PARTITION BY site_id)
)
-- Subtotals with ROLLUP
SELECT
    rn,
    quality,
    GROUPING(rn) AS is_total_rn,
    PERCENTILE_APPROX(site_id_min_days_since_read_at, 0.5) AS median_site_id_min_days_since_read_at,
    COUNT(*) FILTER (WHERE quality IS NOT NULL) AS n_quality_is_not_null,
    MIN(next_days_since_read_at) AS min_next_days_since_read_at
FROM ranked
GROUP BY ROLLUP(rn, quality)
