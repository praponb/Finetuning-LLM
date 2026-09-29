WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        COALESCE(kwh, 0.0) AS kwh_filled,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 6 AND 11
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(kwh) OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS kwh_sum_7,
        LAST_VALUE(kwh_filled) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh_filled,
        MAX(days_since_read_at) OVER (PARTITION BY site_id ORDER BY voltage DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS days_since_read_at_max_2_prior
    FROM recent_readings
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at >= TIMESTAMP '2025-02-06 14:00:00'
              AND m.quality IN ('EST', 'BAD')
        )
),
joined_site AS (
    -- Attach site attributes
    SELECT s.*, si.installed_kw AS site_installed_kw
    FROM without_matches s
    LEFT JOIN sites si ON si.id = s.site_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        kwh_sum_7,
        last_known_kwh_filled,
        days_since_read_at_max_2_prior,
        QUARTER(read_at) AS read_at_quarter
    FROM joined_site
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        kwh_sum_7,
        last_known_kwh_filled,
        days_since_read_at_max_2_prior,
        read_at_quarter,
        FLOOR(kwh_sum_7 / 50) * 50 AS kwh_sum_7_bucket,
        ROUND(read_at_quarter / NULLIF(last_known_kwh_filled, 0), 3) AS read_at_quarter_per_last_known_kwh_filled,
        ROUND(last_known_kwh_filled / NULLIF(read_at_quarter, 0), 3) AS last_known_kwh_filled_per_read_at_quarter
    FROM tagged
    WHERE last_known_kwh_filled BETWEEN 42.0 AND 96.0
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM enriched
    WHERE last_known_kwh_filled > (SELECT MIN(last_known_kwh_filled) FROM enriched)
)
SELECT
    id,
    kwh_sum_7,
    last_known_kwh_filled,
    days_since_read_at_max_2_prior,
    read_at_quarter,
    kwh_sum_7_bucket,
    FLOOR(kwh_sum_7 / 50) * 50 AS kwh_sum_7_bucket_2
FROM filtered
ORDER BY kwh_sum_7_bucket_2
