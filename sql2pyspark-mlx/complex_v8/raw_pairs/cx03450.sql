WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
        ABS(kwh - 80.8) AS kwh_distance
    FROM meter_readings
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at >= TIMESTAMP '2025-01-25 10:00:00'
              AND m.voltage > 224.8
        )
      AND s.voltage < 237.5
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        read_at,
        quality_ref,
        quality_prefix,
        SUBSTRING(TRIM(quality_prefix), 1, 4) AS quality_prefix_prefix,
        UPPER(TRIM(quality_prefix)) AS quality_prefix_clean
    FROM filtered
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_read_at AS read_at_dense_rank,
        ROW_NUMBER() OVER w_all_by_read_at AS read_at_seq,
        NTILE(4) OVER (ORDER BY read_at NULLS LAST, id) AS read_at_quartile,
        NTILE(3) OVER (ORDER BY read_at NULLS LAST, id) AS read_at_tercile
    FROM tagged
    WINDOW w_all_by_read_at AS (ORDER BY read_at NULLS LAST, id)
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM sequenced
    WHERE read_at_dense_rank < (SELECT MAX(read_at_dense_rank) FROM sequenced)
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM screened
    WHERE read_at_seq < (SELECT MAX(read_at_seq) FROM screened)
)
SELECT
    id,
    quality_ref,
    quality_prefix,
    quality_prefix_prefix,
    quality_prefix_clean,
    read_at_dense_rank,
    read_at_seq,
    read_at_quartile,
    DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label
FROM with_activity
ORDER BY read_at_label DESC NULLS FIRST, quality_prefix_prefix NULLS LAST
