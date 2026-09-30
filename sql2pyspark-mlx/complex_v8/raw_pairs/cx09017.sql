WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE quality IS NOT NULL
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM valid_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage BETWEEN 215.8 AND 234.7
              AND m.quality = 'BAD'
        )
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality_ref,
        quality_or_unknown,
        ROUND(kwh - AVG(kwh) OVER w_read_at_year, 2) AS kwh_vs_read_at_year_avg,
        ROUND(AVG(kwh) OVER (PARTITION BY YEAR(read_at)), 2) AS read_at_year_avg_kwh
    FROM filtered
    WINDOW w_read_at_year AS (PARTITION BY YEAR(read_at))
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        kwh_vs_read_at_year_avg,
        read_at_year_avg_kwh,
        read_at + INTERVAL 7 DAYS AS read_at_plus_7d,
        COALESCE(quality_or_unknown, 'UNKNOWN') AS quality_or_unknown_or_unknown
    FROM metrics
    WHERE read_at_year_avg_kwh <= 66.0
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(kwh / NULLIF(kwh_vs_read_at_year_avg, 0), 3) AS kwh_per_kwh_vs_read_at_year_avg,
        CASE WHEN voltage >= 233.1 THEN 'large' WHEN voltage >= 223.3 THEN 'medium' ELSE 'small' END AS voltage_tier,
        ROUND(kwh_vs_read_at_year_avg * 1.5, 2) AS kwh_vs_read_at_year_avg_adj
    FROM scored
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        kwh_vs_read_at_year_avg,
        read_at_plus_7d,
        quality_or_unknown_or_unknown,
        kwh_per_kwh_vs_read_at_year_avg,
        kwh_vs_read_at_year_avg_adj,
        SUM(kwh) OVER (PARTITION BY quality_or_unknown_or_unknown) AS quality_or_unknown_or_unknown_sum_kwh,
        SUM(kwh) OVER (ORDER BY voltage, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS kwh_sum_6_prior
    FROM labeled
),
summary AS (
    -- Summarise per group
    SELECT
        TO_DATE(read_at_plus_7d) AS read_at_plus_7d_day,
        quality_or_unknown_or_unknown,
        YEAR(read_at) AS read_at_year,
        ROUND(SUM(quality_or_unknown_or_unknown_sum_kwh) / NULLIF(SUM(kwh_per_kwh_vs_read_at_year_avg), 0), 3) AS quality_or_unknown_or_unknown_sum_kwh_to_kwh_per,
        MAX(read_at_plus_7d) AS last_read_at_plus_7d,
        COUNT(DISTINCT quality_or_unknown_or_unknown) AS distinct_quality_or_unknown_or_unknowns
    FROM ranked
    GROUP BY TO_DATE(read_at_plus_7d), quality_or_unknown_or_unknown, YEAR(read_at)
)
SELECT
    read_at_plus_7d_day,
    quality_or_unknown_or_unknown,
    read_at_year,
    quality_or_unknown_or_unknown_sum_kwh_to_kwh_per,
    last_read_at_plus_7d,
    distinct_quality_or_unknown_or_unknowns
FROM summary
WHERE quality_or_unknown_or_unknown = 'UNKNOWN'
ORDER BY last_read_at_plus_7d, read_at_year NULLS LAST, read_at_plus_7d_day, quality_or_unknown_or_unknown
LIMIT 20
