WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-02-24'
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM cleaned_readings
    WHERE kwh_filled < 102.0
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality_len,
        quality_group,
        ROUND(CUME_DIST() OVER w_quality_group_site_id_by_rea, 3) AS read_at_cume_dist,
        ROUND(kwh_filled / NULLIF(SUM(kwh_filled) OVER w_site_id, 0), 4) AS kwh_filled_share_of_site_id,
        ROUND(PERCENT_RANK() OVER w_quality_group_site_id_by_rea_2, 3) AS read_at_percent_rank,
        NTILE(10) OVER (PARTITION BY site_id, quality_group ORDER BY voltage, id) AS voltage_decile
    FROM tagged
    WINDOW w_quality_group_site_id_by_rea AS (PARTITION BY quality_group, site_id ORDER BY read_at), w_site_id AS (PARTITION BY site_id), w_quality_group_site_id_by_rea_2 AS (PARTITION BY quality_group, site_id ORDER BY read_at, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY quality_group, quality_len ORDER BY read_at_percent_rank) AS read_at_percent_rank_dense_rank,
        DENSE_RANK() OVER w_quality_group_quality_len_by_2 AS read_at_percent_rank_dense_rank_2
    FROM metrics
    WINDOW w_quality_group_quality_len_by_2 AS (PARTITION BY quality_group, quality_len ORDER BY read_at_percent_rank, id)
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality_len,
    voltage_decile,
    read_at_percent_rank_dense_rank,
    read_at_percent_rank_dense_rank_2,
    MONTH(read_at) AS read_at_month,
    COALESCE(read_at_percent_rank, 0.0) AS read_at_percent_rank_filled
FROM with_trends
ORDER BY quality_len DESC, kwh
