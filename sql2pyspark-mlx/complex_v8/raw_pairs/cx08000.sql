WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
        CASE WHEN kwh >= 79.4 THEN 'A' WHEN kwh >= 50.4 THEN 'B' ELSE 'C' END AS kwh_tier
    FROM meter_readings
    WHERE kwh BETWEEN 34.6 AND 118.3
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        quality_prefix,
        FIRST_VALUE(kwh) OVER (PARTITION BY site_id ORDER BY kwh ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY quality), 0), 4) AS voltage_share_of_quality,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        DENSE_RANK() OVER (PARTITION BY site_id ORDER BY kwh, id) AS kwh_dense_rank,
        ROUND(CUME_DIST() OVER w_site_id_by_kwh_2, 3) AS kwh_cume_dist,
        NTILE(5) OVER (PARTITION BY site_id ORDER BY kwh ASC NULLS LAST, id) AS kwh_quintile,
        COUNT(*) OVER w_kwh_tier AS rows_in_kwh_tier,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality), 0), 4) AS kwh_share_of_quality,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality), 2) AS kwh_vs_quality_avg,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY kwh_tier), 2) AS voltage_vs_kwh_tier_avg,
        ROUND(CUME_DIST() OVER (ORDER BY read_at), 3) AS read_at_cume_dist,
        MIN(voltage) OVER (PARTITION BY site_id ORDER BY kwh ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_voltage_so_far,
        ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh, id) AS rn
    FROM recent_readings
    WINDOW w_site_id_by_kwh_2 AS (PARTITION BY site_id ORDER BY kwh ASC NULLS LAST, id), w_kwh_tier AS (PARTITION BY kwh_tier)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS('|', quality_prefix, quality) AS quality_prefix_quality_label,
        ABS(kwh_vs_quality_avg - -48.0) AS kwh_vs_quality_avg_distance
    FROM windowed
),
windowed_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        quality_prefix,
        kwh_dense_rank,
        kwh_cume_dist,
        voltage_vs_kwh_tier_avg,
        read_at_cume_dist,
        rn,
        quality_prefix_quality_label,
        kwh_vs_quality_avg_distance,
        ROUND(kwh_vs_quality_avg / NULLIF(SUM(kwh_vs_quality_avg) OVER (PARTITION BY rn), 0), 4) AS kwh_vs_quality_avg_share_of_rn,
        ROW_NUMBER() OVER (PARTITION BY quality, rn ORDER BY voltage_vs_kwh_tier_avg ASC, id) AS seq_no,
        FIRST_VALUE(kwh) OVER (ORDER BY kwh_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh_2,
        MAX(first_kwh) OVER (PARTITION BY MONTH(second_read_at) ORDER BY second_read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_first_kwh_so_far
    FROM tagged
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        seq_no,
        ROUND(100 * COUNT(CASE WHEN quality = 'OK' THEN 1 END) / COUNT(*), 1) AS pct_quality_ok,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(read_at_cume_dist) AS min_read_at_cume_dist,
        ROUND(SUM(kwh_cume_dist), 2) AS total_kwh_cume_dist
    FROM windowed_2
    GROUP BY DATE_TRUNC('MONTH', read_at), seq_no
    HAVING COUNT(*) >= 2
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(distinct_ids / NULLIF(SUM(distinct_ids) OVER (PARTITION BY seq_no), 0), 4) AS distinct_ids_share_of_seq_no,
        pct_quality_ok - LAG(pct_quality_ok) OVER w_all_by_min_read_at_cume_dist AS pct_quality_ok_change
    FROM totals
    WINDOW w_all_by_min_read_at_cume_dist AS (ORDER BY min_read_at_cume_dist, read_at_month, seq_no)
)
SELECT
    read_at_month,
    cnt,
    distinct_ids,
    min_read_at_cume_dist,
    total_kwh_cume_dist,
    distinct_ids_share_of_seq_no,
    pct_quality_ok_change,
    FLOOR(pct_quality_ok / 25) * 25 AS pct_quality_ok_bucket
FROM ranked
WHERE distinct_ids >= 2
