WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
),
with_details_site AS (
    -- Attach site attributes
    SELECT
        s.*,
        si.site_name,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM recent s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on < DATE '2025-05-07'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY site_id, site_tariff) AS rows_in_site_id_site_tariff,
        NTH_VALUE(site_commissioned_on, 2) OVER (ORDER BY site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_site_commissioned_on,
        DENSE_RANK() OVER (ORDER BY kwh ASC NULLS LAST) AS kwh_dense_rank,
        SIZE(COLLECT_SET(site_tariff) OVER w_quality_site_id) AS distinct_site_tariff_in_quality_site_id,
        ROUND(AVG(kwh_bucket) OVER (PARTITION BY TO_DATE(read_at), site_tariff ORDER BY voltage, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS kwh_bucket_avg_4,
        RANK() OVER (ORDER BY site_commissioned_on) AS site_commissioned_on_rank,
        ROUND(kwh_bucket - AVG(kwh_bucket) OVER (PARTITION BY site_id, site_tariff), 2) AS kwh_bucket_vs_site_id_site_tariff_avg,
        DENSE_RANK() OVER (ORDER BY kwh ASC NULLS LAST, id) AS kwh_dense_rank_2,
        voltage - LAG(voltage) OVER w_all_by_kwh AS voltage_change
    FROM with_details_site
    WINDOW w_quality_site_id AS (PARTITION BY quality, site_id), w_all_by_kwh AS (ORDER BY kwh ASC NULLS LAST, id)
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at_label,
        kwh_bucket,
        site_name,
        site_commissioned_on,
        site_tariff,
        rows_in_site_id_site_tariff,
        second_site_commissioned_on,
        kwh_dense_rank,
        distinct_site_tariff_in_quality_site_id,
        kwh_bucket_avg_4,
        site_commissioned_on_rank,
        kwh_bucket_vs_site_id_site_tariff_avg,
        voltage_change
    FROM metrics
    WHERE kwh_dense_rank_2 <= 3
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        kwh,
        voltage,
        quality,
        kwh_bucket,
        site_name,
        site_commissioned_on,
        site_tariff,
        rows_in_site_id_site_tariff,
        second_site_commissioned_on,
        kwh_dense_rank,
        distinct_site_tariff_in_quality_site_id,
        kwh_bucket_avg_4,
        site_commissioned_on_rank,
        kwh_bucket_vs_site_id_site_tariff_avg,
        voltage_change,
        SUM(site_commissioned_on_rank) OVER (PARTITION BY site_id, site_tariff) AS site_id_site_tariff_sum_site_commissioned_on_ran,
        SUM(rows_in_site_id_site_tariff) OVER (ORDER BY distinct_site_tariff_in_quality_site_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rows_in_site_id_site_tariff,
        LAST_VALUE(site_commissioned_on_rank) IGNORE NULLS OVER (PARTITION BY site_tariff, distinct_site_tariff_in_quality_site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_site_commissioned_on_rank,
        SUM(voltage) OVER (PARTITION BY site_tariff, quality) AS site_tariff_quality_sum_voltage,
        NTILE(5) OVER (ORDER BY distinct_site_tariff_in_quality_site_id, id) AS distinct_site_tariff_in_quality_site_id_quintile,
        LEAD(site_commissioned_on, 3) OVER (PARTITION BY site_tariff, distinct_site_tariff_in_quality_site_id ORDER BY voltage, id) AS next3_site_commissioned_on
    FROM first_rows
),
summary AS (
    -- Group-level totals
    SELECT
        site_commissioned_on_rank,
        last_known_site_commissioned_on_rank,
        SUM(running_rows_in_site_id_site_tariff) AS total_running_rows_in_site_id_site_tariff,
        MIN(next3_site_commissioned_on) AS first_next3_site_commissioned_on
    FROM metrics_2
    WHERE rows_in_site_id_site_tariff > 8
    GROUP BY 1, 2
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(site_commissioned_on_rank / NULLIF(SUM(site_commissioned_on_rank) OVER w_site_commissioned_on_rank, 0), 4) AS site_commissioned_on_rank_share_of_site_commissi,
        ROW_NUMBER() OVER (ORDER BY total_running_rows_in_site_id_site_tariff, site_commissioned_on_rank, last_known_site_commissioned_on_rank) AS rn,
        DENSE_RANK() OVER (PARTITION BY site_commissioned_on_rank ORDER BY first_next3_site_commissioned_on DESC, site_commissioned_on_rank, last_known_site_commissioned_on_rank) AS first_next3_site_commissioned_on_dense_rank,
        COUNT(*) OVER (PARTITION BY site_commissioned_on_rank) AS rows_in_site_commissioned_on_rank
    FROM summary
    WINDOW w_site_commissioned_on_rank AS (PARTITION BY site_commissioned_on_rank)
)
SELECT
    site_commissioned_on_rank,
    last_known_site_commissioned_on_rank,
    first_next3_site_commissioned_on,
    site_commissioned_on_rank_share_of_site_commissi,
    rn,
    first_next3_site_commissioned_on_dense_rank,
    rows_in_site_commissioned_on_rank,
    COALESCE(rn, 0) AS rn_filled,
    ABS(rn - 7) AS rn_distance
FROM with_windows
ORDER BY site_commissioned_on_rank_share_of_site_commissi NULLS LAST
