WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 7 AND 10
      AND quality IN ('EST', 'BAD')
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_id), 2) AS kwh_vs_site_id_avg,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY quality_or_unknown, site_id), 2) AS voltage_vs_quality_or_unknown_site_id_avg,
        DENSE_RANK() OVER w_quality_or_unknown_read_at_t AS voltage_dense_rank,
        ROUND(AVG(kwh) OVER (PARTITION BY quality_or_unknown, TO_DATE(read_at) ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_7,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id ORDER BY read_at DESC, id), 3) AS read_at_percent_rank,
        SIZE(COLLECT_SET(quality_or_unknown) OVER (PARTITION BY site_id)) AS distinct_quality_or_unknown_in_site_id,
        MAX(kwh) OVER w_quality_or_unknown_site_id AS quality_or_unknown_site_id_max_kwh,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY quality_or_unknown, TO_DATE(read_at) ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY site_id ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id), 0), 4) AS voltage_share_of_site_id,
        LAG(voltage) OVER (PARTITION BY site_id, YEAR(read_at) ORDER BY kwh NULLS LAST, id) AS prev_voltage,
        MIN(kwh) OVER (PARTITION BY site_id) AS site_id_min_kwh,
        DENSE_RANK() OVER (PARTITION BY quality_or_unknown ORDER BY voltage ASC) AS voltage_dense_rank_2,
        ROW_NUMBER() OVER (PARTITION BY quality_or_unknown, TO_DATE(read_at) ORDER BY voltage DESC NULLS FIRST, id) AS voltage_seq,
        DENSE_RANK() OVER w_site_id_by_read_at_ties AS read_at_dense_rank
    FROM base
    WINDOW w_quality_or_unknown_read_at_t AS (PARTITION BY quality_or_unknown, TO_DATE(read_at) ORDER BY voltage DESC NULLS FIRST), w_quality_or_unknown_site_id AS (PARTITION BY quality_or_unknown, site_id), w_site_id_by_read_at_ties AS (PARTITION BY site_id ORDER BY read_at DESC)
),
leaders AS (
    -- Top 5 per group
    SELECT *
    FROM sequenced
    WHERE voltage_dense_rank <= 5
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality_or_unknown,
    kwh_vs_site_id_avg,
    voltage_vs_quality_or_unknown_site_id_avg,
    voltage_dense_rank,
    kwh_avg_7,
    read_at_percent_rank,
    first_known_read_at,
    prev_voltage,
    read_at_dense_rank,
    MONTH(read_at) AS read_at_month
FROM leaders
ORDER BY id DESC, read_at_dense_rank
