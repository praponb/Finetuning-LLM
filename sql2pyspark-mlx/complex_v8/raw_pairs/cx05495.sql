WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE voltage >= 218.5
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(read_at) ORDER BY kwh NULLS LAST, id) AS kwh_seq,
        ROUND(CUME_DIST() OVER w_site_id_by_read_at, 3) AS read_at_cume_dist,
        COUNT(*) OVER (PARTITION BY TO_DATE(read_at), site_id) AS rows_in_read_at_to_date_site_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY read_at ASC), 3) AS read_at_percent_rank,
        MIN(voltage) OVER (PARTITION BY site_id) AS site_id_min_voltage,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_site_id, 0), 4) AS kwh_share_of_site_id,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(read_at), site_id ORDER BY read_at DESC, id), 3) AS read_at_cume_dist_2,
        LEAD(kwh, 1, 0) OVER (PARTITION BY site_id ORDER BY read_at ASC, id) AS next_kwh,
        RANK() OVER (PARTITION BY site_id ORDER BY read_at ASC) AS read_at_rank,
        LAG(voltage, 1, 0) OVER w_site_id_by_read_at AS prev_voltage,
        LEAD(kwh, 2) OVER (PARTITION BY site_id ORDER BY read_at ASC, id) AS next2_kwh
    FROM scoped
    WINDOW w_site_id_by_read_at AS (PARTITION BY site_id ORDER BY read_at DESC, id), w_site_id AS (PARTITION BY site_id)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        YEAR(read_at) AS read_at_year,
        ROUND(read_at_percent_rank * 0.9, 2) AS read_at_percent_rank_adj,
        ABS(voltage - 232.0) AS voltage_distance
    FROM sequenced
    WHERE read_at_cume_dist < 0.7
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality_ref,
    read_at_cume_dist,
    read_at_percent_rank,
    kwh_share_of_site_id,
    next_kwh,
    read_at_rank,
    prev_voltage,
    next2_kwh,
    read_at_year,
    read_at_percent_rank_adj,
    voltage_distance
FROM derived
ORDER BY next2_kwh, kwh_share_of_site_id DESC
