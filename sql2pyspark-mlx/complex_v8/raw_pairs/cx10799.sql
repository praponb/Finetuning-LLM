WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh BETWEEN 15.9 AND 96.8
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY site_id ORDER BY voltage DESC) AS voltage_rank,
        MAX(kwh) OVER (PARTITION BY site_id ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far,
        ROW_NUMBER() OVER (PARTITION BY site_id, quality ORDER BY voltage, id) AS voltage_seq,
        SUM(voltage) OVER (PARTITION BY site_id, TO_DATE(read_at)) AS site_id_read_at_to_date_sum_voltage
    FROM filtered_readings
    WINDOW w_site_id_read_at_to_date AS (PARTITION BY site_id, TO_DATE(read_at))
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        voltage,
        quality,
        voltage_rank,
        max_kwh_so_far,
        voltage_seq,
        ROUND(AVG(voltage_seq) OVER (PARTITION BY voltage_rank ORDER BY voltage_rank DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS voltage_seq_avg_4,
        ROW_NUMBER() OVER (PARTITION BY voltage_rank ORDER BY voltage_seq, id) AS rn,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY voltage_rank ORDER BY voltage_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        COUNT(*) OVER (PARTITION BY site_id, voltage_rank) AS rows_in_site_id_voltage_rank
    FROM with_trends
),
enriched AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
    FROM metrics
    WHERE second_kwh < 87.1
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(max_kwh_so_far, 2, 0) OVER (PARTITION BY quality ORDER BY voltage, id) AS next2_max_kwh_so_far,
        ROUND(voltage - AVG(voltage) OVER w_quality_read_at_month, 2) AS voltage_vs_quality_read_at_month_avg
    FROM enriched
    WINDOW w_quality_read_at_month AS (PARTITION BY quality, DATE_TRUNC('MONTH', read_at))
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM windowed
    WHERE rn > (SELECT MIN(rn) FROM windowed)
),
deduped AS (
    -- Keep the latest row per quality
    SELECT
        id,
        read_at,
        voltage,
        quality,
        voltage_rank,
        max_kwh_so_far,
        voltage_seq,
        voltage_seq_avg_4,
        rn,
        second_kwh,
        rows_in_site_id_voltage_rank,
        quality_prefix,
        next2_max_kwh_so_far,
        voltage_vs_quality_read_at_month_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY max_kwh_so_far DESC, id DESC) AS row_num
        FROM qualified
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    read_at,
    voltage,
    quality,
    voltage_rank,
    max_kwh_so_far,
    voltage_seq,
    voltage_seq_avg_4,
    rn,
    rows_in_site_id_voltage_rank,
    quality_prefix,
    voltage_vs_quality_read_at_month_avg,
    TO_DATE(read_at) AS read_at_date,
    LENGTH(TRIM(quality)) AS quality_len
FROM deduped
ORDER BY voltage_vs_quality_read_at_month_avg DESC NULLS FIRST
