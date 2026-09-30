WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE quality = 'BAD'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on,
        si.site_name
    FROM cleaned_readings s
    JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        site_tariff,
        site_name,
        MAX(kwh) OVER (PARTITION BY site_tariff, site_id) AS site_tariff_site_id_max_kwh,
        RANK() OVER (PARTITION BY site_id, site_tariff ORDER BY site_commissioned_on) AS site_commissioned_on_rank,
        LAG(read_at) OVER (PARTITION BY site_id ORDER BY kwh ASC, id) AS prev_read_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id, site_tariff ORDER BY site_commissioned_on), 3) AS site_commissioned_on_percent_rank,
        ROUND(kwh - AVG(kwh) OVER w_site_tariff_site_id, 2) AS kwh_vs_site_tariff_site_id_avg,
        SUM(kwh) OVER (PARTITION BY site_id, site_tariff ORDER BY site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh,
        COUNT(*) OVER (PARTITION BY site_tariff, site_id) AS rows_in_site_tariff_site_id,
        SUM(kwh) OVER (PARTITION BY site_id ORDER BY kwh ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh_2,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id ORDER BY kwh ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_7,
        MIN(kwh) OVER (PARTITION BY site_tariff, site_id) AS site_tariff_site_id_min_kwh,
        ROUND(PERCENT_RANK() OVER w_site_id_by_site_commissioned, 3) AS site_commissioned_on_percent_rank_2,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id, site_tariff ORDER BY site_commissioned_on, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_1_prior,
        SUM(kwh) OVER (PARTITION BY site_id ORDER BY site_commissioned_on, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS kwh_sum_6_prior
    FROM enriched
    WINDOW w_site_tariff_site_id AS (PARTITION BY site_tariff, site_id), w_site_id_by_site_commissioned AS (PARTITION BY site_id ORDER BY site_commissioned_on)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        read_at,
        kwh,
        site_tariff,
        site_name,
        site_tariff_site_id_max_kwh,
        site_commissioned_on_rank,
        prev_read_at,
        running_kwh,
        rows_in_site_tariff_site_id,
        running_kwh_2,
        kwh_avg_7,
        site_tariff_site_id_min_kwh,
        site_commissioned_on_percent_rank_2,
        kwh_sum_6_prior,
        DATEDIFF(DATE '2025-06-30', prev_read_at) AS days_since_prev_read_at,
        FLOOR(kwh / 25) * 25 AS kwh_bucket,
        CASE WHEN site_tariff IN ('TOU', 'FLAT') THEN 'group_a' WHEN site_tariff = 'EV' THEN 'group_b' ELSE 'other' END AS site_tariff_group
    FROM with_trends
),
with_trends_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY site_commissioned_on_rank) AS rows_in_site_commissioned_on_rank,
        NTH_VALUE(days_since_prev_read_at, 2) OVER (PARTITION BY site_commissioned_on_rank ORDER BY prev_read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_prev_read_at,
        SUM(running_kwh) OVER (PARTITION BY site_commissioned_on_rank) AS site_commissioned_on_rank_sum_running_kwh,
        ROUND(running_kwh_2 / NULLIF(SUM(running_kwh_2) OVER (PARTITION BY site_commissioned_on_rank), 0), 4) AS running_kwh_2_share_of_site_commissioned_on_rank,
        DENSE_RANK() OVER (PARTITION BY site_commissioned_on_rank ORDER BY prev_read_at DESC) AS prev_read_at_dense_rank,
        MIN(kwh_sum_6_prior) OVER (PARTITION BY site_commissioned_on_rank) AS site_commissioned_on_rank_min_kwh_sum_6_prior
    FROM tagged
),
per_group AS (
    -- Group-level totals
    SELECT
        TO_DATE(prev_read_at) AS prev_read_at_day,
        ROUND(SUM(running_kwh), 2) AS total_running_kwh,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT site_tariff) AS distinct_site_tariffs
    FROM with_trends_2
    WHERE site_tariff_site_id_max_kwh >= 420.0
    GROUP BY TO_DATE(prev_read_at)
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    prev_read_at_day,
    total_running_kwh,
    n_rows,
    distinct_ids,
    distinct_site_tariffs,
    ROUND(distinct_site_tariffs / NULLIF(distinct_ids, 0), 3) AS distinct_site_tariffs_per_distinct_ids,
    GREATEST(n_rows, distinct_site_tariffs) AS greatest_n_rows_distinct_site_tariffs
FROM per_group
WHERE prev_read_at_day < DATE '2025-06-01'
ORDER BY n_rows
