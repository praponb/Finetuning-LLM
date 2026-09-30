WITH recent_readings AS (
    -- Filter meter_readings
    SELECT *
    FROM meter_readings
    WHERE quality IS NOT NULL
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        si.site_name
    FROM recent_readings s
    LEFT JOIN sites si ON si.id = s.site_id AND si.zone = 'Z3'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        voltage,
        quality,
        site_name,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        LEAD(kwh) OVER (ORDER BY kwh, id) AS next_kwh,
        MAX(kwh) OVER (ORDER BY voltage ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS kwh_max_3_prior,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at)), 2) AS kwh_vs_site_id_read_at_month_avg,
        COUNT(*) OVER (ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY kwh DESC, id) AS kwh_seq,
        kwh - LAG(kwh) OVER (ORDER BY voltage ASC, id) AS kwh_change,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at))) AS distinct_quality_in_site_id_read_at_month,
        ROUND(AVG(kwh) OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY kwh DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_2_prior,
        ROUND(CUME_DIST() OVER w_read_at_to_date_quality_by_k_2, 3) AS kwh_cume_dist,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        COUNT(*) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at)) AS rows_in_site_id_read_at_month,
        RANK() OVER (ORDER BY voltage ASC, id) AS voltage_rank,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', read_at)), 0), 4) AS voltage_share_of_site_id_read_at_month
    FROM combined_details_site
    WINDOW w_read_at_to_date_quality_by_k_2 AS (PARTITION BY TO_DATE(read_at), quality ORDER BY kwh DESC, id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(voltage_rank) OVER (PARTITION BY YEAR(second_read_at), quality ORDER BY voltage_share_of_site_id_read_at_month DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS voltage_rank_avg_3,
        LAG(read_at) OVER (PARTITION BY YEAR(second_read_at), quality ORDER BY voltage_share_of_site_id_read_at_month DESC, id) AS prev_read_at,
        MIN(kwh_cume_dist) OVER w_distinct_quality_in_site_id AS distinct_quality_in_site_id_read_at_month_kwh_se,
        NTH_VALUE(kwh_seq, 2) OVER (PARTITION BY YEAR(second_read_at), quality ORDER BY voltage_share_of_site_id_read_at_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh_seq
    FROM metrics
    WINDOW w_distinct_quality_in_site_id AS (PARTITION BY distinct_quality_in_site_id_read_at_month, kwh_seq)
)
-- Final projection
SELECT
    id,
    voltage,
    quality,
    site_name,
    second_voltage,
    next_kwh,
    kwh_max_3_prior,
    kwh_vs_site_id_read_at_month_avg,
    running_count,
    kwh_seq,
    kwh_change,
    distinct_quality_in_site_id_read_at_month,
    second_read_at,
    rows_in_site_id_read_at_month,
    voltage_rank,
    voltage_share_of_site_id_read_at_month,
    voltage_rank_avg_3,
    prev_read_at,
    distinct_quality_in_site_id_read_at_month_kwh_se,
    second_kwh_seq,
    LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
FROM windowed
WHERE MONTH(prev_read_at) IN (1, 2)
ORDER BY site_name DESC, kwh_seq, id
LIMIT 10
