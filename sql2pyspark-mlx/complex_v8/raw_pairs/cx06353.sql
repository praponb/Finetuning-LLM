WITH filtered_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage <= 231.6
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        SIZE(COLLECT_SET(quality) OVER w_site_id) AS distinct_quality_in_site_id,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        ROUND(AVG(voltage) OVER (ORDER BY voltage NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_1_prior,
        MIN(voltage) OVER w_site_id_read_at_to_date AS site_id_read_at_to_date_min_voltage,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY read_at), 3) AS read_at_cume_dist,
        RANK() OVER (PARTITION BY site_id ORDER BY read_at, id) AS read_at_rank,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality, site_id), 0), 4) AS voltage_share_of_quality_site_id,
        ROW_NUMBER() OVER (ORDER BY voltage NULLS LAST, id) AS voltage_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY kwh, id), 3) AS kwh_percent_rank,
        MAX(voltage) OVER (PARTITION BY quality, site_id) AS quality_site_id_max_voltage,
        ROUND(CUME_DIST() OVER (ORDER BY voltage NULLS LAST), 3) AS voltage_cume_dist,
        MIN(voltage) OVER (ORDER BY voltage NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_voltage_so_far,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id, TO_DATE(read_at)), 0), 4) AS voltage_share_of_site_id_read_at_to_date,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id, TO_DATE(read_at))) AS distinct_quality_in_site_id_read_at_to_date,
        ROUND(AVG(voltage) OVER (PARTITION BY quality, site_id), 2) AS quality_site_id_avg_voltage,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id
    FROM filtered_readings
    WINDOW w_site_id AS (PARTITION BY site_id), w_site_id_read_at_to_date AS (PARTITION BY site_id, TO_DATE(read_at))
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY site_id, read_at_rank ORDER BY second_kwh, id) AS seq_no,
        LEAD(read_at_rank, 1, 0) OVER (PARTITION BY distinct_quality_in_site_id ORDER BY voltage_share_of_quality_site_id DESC, id) AS next_read_at_rank,
        NTILE(5) OVER (PARTITION BY distinct_quality_in_site_id ORDER BY voltage_share_of_quality_site_id DESC, id) AS voltage_share_of_quality_site_id_quintile,
        MAX(kwh_percent_rank) OVER (PARTITION BY distinct_quality_in_site_id ORDER BY voltage_share_of_quality_site_id DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS kwh_percent_rank_max_2
    FROM with_trends
)
SELECT
    id,
    site_id,
    distinct_quality_in_site_id,
    second_kwh,
    voltage_avg_1_prior,
    site_id_read_at_to_date_min_voltage,
    read_at_rank,
    voltage_share_of_quality_site_id,
    voltage_seq,
    voltage_cume_dist,
    min_voltage_so_far,
    distinct_quality_in_site_id_read_at_to_date,
    rows_in_site_id,
    seq_no,
    voltage_share_of_quality_site_id_quintile,
    kwh_percent_rank_max_2,
    ROUND(kwh * 0.9, 2) AS kwh_adj,
    MONTH(read_at) AS read_at_month
FROM sequenced
WHERE read_at_rank >= 5
ORDER BY min_voltage_so_far, site_id_read_at_to_date_min_voltage DESC NULLS FIRST, id
LIMIT 10
