WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id <= 9
      AND quality <> 'BAD'
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        si.tariff AS site_tariff
    FROM base s
    JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY quality), 2) AS kwh_vs_quality_avg,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        ROUND(AVG(voltage) OVER (ORDER BY read_at ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_1_prior,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality), 0), 4) AS kwh_share_of_quality,
        LAST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        NTH_VALUE(voltage, 2) OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        FIRST_VALUE(kwh) OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh,
        DENSE_RANK() OVER (ORDER BY read_at ASC) AS read_at_dense_rank,
        COUNT(*) OVER (PARTITION BY quality ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality,
        LAST_VALUE(read_at) OVER (ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at,
        LAG(kwh) OVER (ORDER BY voltage ASC, id) AS prev_kwh
    FROM with_details_site
    WINDOW w_site_id AS (PARTITION BY site_id)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        site_tariff,
        kwh_vs_quality_avg,
        rows_in_site_id,
        voltage_avg_1_prior,
        kwh_share_of_quality,
        last_known_read_at,
        kwh_share_of_site_id,
        first_kwh,
        read_at_dense_rank,
        running_count,
        distinct_site_id_in_quality,
        last_read_at,
        ROUND(kwh / NULLIF(read_at_dense_rank, 0), 3) AS kwh_per_read_at_dense_rank
    FROM with_trends
    WHERE distinct_site_id_in_quality <= 8
)
SELECT
    id,
    read_at,
    kwh,
    voltage,
    site_tariff,
    kwh_vs_quality_avg,
    rows_in_site_id,
    kwh_share_of_site_id,
    first_kwh,
    read_at_dense_rank,
    running_count,
    distinct_site_id_in_quality,
    last_read_at,
    kwh_per_read_at_dense_rank
FROM labeled
WHERE last_read_at < TIMESTAMP '2025-02-24 10:00:00'
ORDER BY last_read_at DESC NULLS FIRST, id
LIMIT 20
