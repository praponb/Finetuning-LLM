WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage >= 218.5
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        kwh,
        voltage,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM recent
    WHERE quality IN ('EST', 'OK')
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        UPPER(TRIM(read_at_label)) AS read_at_label_clean,
        CONCAT(read_at_label, '#', CAST(days_since_read_at AS STRING)) AS read_at_label_ref
    FROM enriched
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        kwh,
        days_since_read_at,
        read_at_label_clean,
        read_at_label_ref,
        LAST_VALUE(voltage) OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage,
        DENSE_RANK() OVER w_all_by_voltage_ties AS voltage_dense_rank
    FROM scored
    WINDOW w_all_by_voltage_ties AS (ORDER BY voltage DESC)
),
scored_2 AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(read_at_label_ref), ' ', '_')) AS read_at_label_ref_slug
    FROM with_trends
    WHERE kwh > 46.6
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        kwh,
        days_since_read_at,
        last_voltage,
        voltage_dense_rank,
        read_at_label_ref_slug,
        COUNT(*) OVER w_voltage_dense_rank AS rows_in_voltage_dense_rank,
        ROUND(voltage_dense_rank / NULLIF(SUM(voltage_dense_rank) OVER (PARTITION BY voltage_dense_rank), 0), 4) AS voltage_dense_rank_share_of_voltage_dense_rank,
        MIN(days_since_read_at) OVER (PARTITION BY voltage_dense_rank ORDER BY voltage_dense_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_days_since_read_at_so_far,
        ROUND(PERCENT_RANK() OVER (PARTITION BY voltage_dense_rank ORDER BY voltage_dense_rank DESC), 3) AS voltage_dense_rank_percent_rank,
        ROUND(last_voltage / NULLIF(SUM(last_voltage) OVER (PARTITION BY voltage_dense_rank), 0), 4) AS last_voltage_share_of_voltage_dense_rank,
        LAST_VALUE(last_voltage) IGNORE NULLS OVER (PARTITION BY voltage_dense_rank ORDER BY voltage_dense_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_voltage
    FROM scored_2
    WINDOW w_voltage_dense_rank AS (PARTITION BY voltage_dense_rank)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        kwh,
        days_since_read_at,
        last_voltage,
        read_at_label_ref_slug,
        rows_in_voltage_dense_rank,
        voltage_dense_rank_share_of_voltage_dense_rank,
        min_days_since_read_at_so_far,
        voltage_dense_rank_percent_rank,
        last_voltage_share_of_voltage_dense_rank,
        last_known_last_voltage
    FROM ranked
    WHERE voltage_dense_rank = 1
)
-- Final projection
SELECT
    id,
    kwh,
    days_since_read_at,
    last_voltage,
    read_at_label_ref_slug,
    rows_in_voltage_dense_rank,
    voltage_dense_rank_share_of_voltage_dense_rank,
    min_days_since_read_at_so_far,
    last_voltage_share_of_voltage_dense_rank,
    last_known_last_voltage,
    LENGTH(TRIM(read_at_label_ref_slug)) AS read_at_label_ref_slug_len
FROM leaders
ORDER BY last_voltage, last_voltage_share_of_voltage_dense_rank
