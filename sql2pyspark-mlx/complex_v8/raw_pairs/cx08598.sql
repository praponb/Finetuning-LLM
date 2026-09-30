WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        read_at + INTERVAL 30 MINUTES AS read_at_plus_30m
    FROM meter_readings
    WHERE voltage BETWEEN 219.5 AND 233.8
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(quality_len / NULLIF(SUM(quality_len) OVER (PARTITION BY quality), 0), 4) AS quality_len_share_of_quality,
        ROUND(AVG(quality_len) OVER (PARTITION BY quality_len, MONTH(read_at_plus_30m)), 2) AS quality_len_read_at_plus_30m_month_avg_quality_l,
        SUM(kwh) OVER (PARTITION BY quality) AS quality_sum_kwh,
        LEAD(read_at_plus_30m) OVER (PARTITION BY quality_len, site_id ORDER BY voltage, id) AS next_read_at_plus_30m,
        SUM(quality_len) OVER (ORDER BY read_at_plus_30m, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quality_len,
        ROUND(CUME_DIST() OVER w_quality_len_site_id_by_volta_2, 3) AS voltage_cume_dist,
        MAX(kwh) OVER (PARTITION BY site_id, quality_len ORDER BY read_at_plus_30m NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS kwh_max_2_prior,
        LAST_VALUE(kwh) OVER (ORDER BY read_at_plus_30m DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        COUNT(*) OVER (PARTITION BY quality_len, MONTH(read_at_plus_30m)) AS rows_in_quality_len_read_at_plus_30m_month,
        COUNT(*) OVER w_quality AS rows_in_quality,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY read_at_plus_30m DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        RANK() OVER w_quality_len_site_id_by_volta AS voltage_rank,
        MAX(kwh) OVER (PARTITION BY quality_len, site_id ORDER BY voltage, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS kwh_max_2_prior_2,
        ROW_NUMBER() OVER (PARTITION BY quality_len, site_id ORDER BY voltage, id) AS voltage_seq,
        ROUND(quality_len / NULLIF(SUM(quality_len) OVER (PARTITION BY quality_len, MONTH(read_at_plus_30m)), 0), 4) AS quality_len_share_of_quality_len_read_at_plus_30,
        COUNT(*) OVER w_read_at_plus_30m_month AS rows_in_read_at_plus_30m_month
    FROM prepared
    WINDOW w_quality_len_site_id_by_volta_2 AS (PARTITION BY quality_len, site_id ORDER BY voltage), w_quality AS (PARTITION BY quality), w_quality_len_site_id_by_volta AS (PARTITION BY quality_len, site_id ORDER BY voltage, id), w_read_at_plus_30m_month AS (PARTITION BY DATE_TRUNC('MONTH', read_at_plus_30m))
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality_len,
    read_at_plus_30m,
    quality_len_share_of_quality,
    quality_sum_kwh,
    next_read_at_plus_30m,
    running_quality_len,
    kwh_max_2_prior,
    last_kwh,
    rows_in_quality_len_read_at_plus_30m_month,
    rows_in_quality,
    first_known_kwh,
    voltage_rank,
    kwh_max_2_prior_2,
    rows_in_read_at_plus_30m_month,
    SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
FROM with_trends
ORDER BY kwh_max_2_prior, read_at_plus_30m DESC NULLS FIRST
