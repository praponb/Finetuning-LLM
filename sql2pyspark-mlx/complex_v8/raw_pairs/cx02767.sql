WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        FLOOR(kwh / 25) * 25 AS kwh_bucket,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE quality IN ('BAD', 'EST')
      AND site_id IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(kwh) OVER (PARTITION BY MONTH(read_at), site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far,
        ROUND(AVG(kwh) OVER (PARTITION BY MONTH(read_at), site_id ORDER BY voltage, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_2_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(read_at), site_id ORDER BY voltage, id), 3) AS voltage_percent_rank,
        DENSE_RANK() OVER (ORDER BY kwh_bucket DESC) AS kwh_bucket_dense_rank,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_id), 2) AS kwh_vs_site_id_avg,
        ROW_NUMBER() OVER w_site_id_by_read_at AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY MONTH(read_at), site_id ORDER BY voltage, id) AS voltage_seq,
        ROUND(kwh_bucket - AVG(kwh_bucket) OVER (PARTITION BY site_id), 2) AS kwh_bucket_vs_site_id_avg,
        LAG(kwh, 2) OVER (PARTITION BY site_id ORDER BY read_at DESC NULLS FIRST, id) AS prev2_kwh,
        MAX(voltage) OVER (PARTITION BY site_id ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS voltage_max_3,
        FIRST_VALUE(read_at) OVER (PARTITION BY MONTH(read_at), site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at,
        NTILE(10) OVER (PARTITION BY MONTH(read_at), site_id ORDER BY voltage, id) AS voltage_decile
    FROM cleaned_readings
    WINDOW w_site_id_by_read_at AS (PARTITION BY site_id ORDER BY read_at DESC NULLS FIRST, id)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        voltage_decile,
        DATE_TRUNC('MONTH', first_read_at) AS first_read_at_month,
        kwh_bucket_dense_rank,
        ROUND(SUM(voltage_percent_rank) / NULLIF(SUM(kwh), 0), 3) AS voltage_percent_rank_to_kwh_ratio,
        COUNT(*) AS cnt
    FROM ranked
    WHERE site_id IS NOT NULL
    GROUP BY voltage_decile, DATE_TRUNC('MONTH', first_read_at), kwh_bucket_dense_rank
    HAVING SUM(kwh_bucket_dense_rank) > 4
)
SELECT
    voltage_decile,
    kwh_bucket_dense_rank,
    voltage_percent_rank_to_kwh_ratio,
    cnt,
    QUARTER(first_read_at_month) AS first_read_at_month_quarter
FROM summary
ORDER BY voltage_decile NULLS LAST, kwh_bucket_dense_rank DESC
