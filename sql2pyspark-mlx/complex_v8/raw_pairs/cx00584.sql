WITH valid_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ABS(voltage - 220.2) AS voltage_distance
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND MONTH(read_at) IN (1, 2)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        read_at,
        quality,
        voltage_distance,
        CASE WHEN kwh >= 78.6 THEN 'large' WHEN kwh >= 42.1 THEN 'medium' ELSE 'small' END AS kwh_tier
    FROM valid_readings
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        quality,
        voltage_distance,
        kwh_tier,
        ROUND(PERCENT_RANK() OVER w_kwh_tier_by_read_at_ties, 3) AS read_at_percent_rank,
        ROW_NUMBER() OVER w_kwh_tier_by_read_at AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY kwh_tier ORDER BY read_at DESC, id), 3) AS read_at_cume_dist
    FROM derived
    WINDOW w_kwh_tier_by_read_at_ties AS (PARTITION BY kwh_tier ORDER BY read_at DESC), w_kwh_tier_by_read_at AS (PARTITION BY kwh_tier ORDER BY read_at DESC, id)
)
-- Final projection
SELECT
    id,
    read_at,
    quality,
    voltage_distance,
    kwh_tier,
    read_at_percent_rank,
    read_at_cume_dist
FROM windowed
ORDER BY read_at_cume_dist
