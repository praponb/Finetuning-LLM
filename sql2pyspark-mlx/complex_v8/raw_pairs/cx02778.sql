WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('DAY', read_at) AS read_at_day
    FROM meter_readings
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.read_at_day,
        si.commissioned_on AS site_commissioned_on,
        si.site_name,
        si.installed_kw AS site_installed_kw
    FROM filtered s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(kwh) OVER (ORDER BY read_at NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS kwh_max_3,
        SUM(kwh) OVER (ORDER BY read_at NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS kwh_sum_7,
        ROUND(CUME_DIST() OVER (ORDER BY read_at NULLS LAST, id), 3) AS read_at_cume_dist,
        DENSE_RANK() OVER (PARTITION BY quality ORDER BY site_commissioned_on) AS site_commissioned_on_dense_rank,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(read_at), quality ORDER BY site_commissioned_on ASC) AS site_commissioned_on_dense_rank_2,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY read_at_day DESC, id) AS read_at_day_quartile,
        ROUND(site_installed_kw - AVG(site_installed_kw) OVER (PARTITION BY TO_DATE(read_at), site_id), 2) AS site_installed_kw_vs_read_at_to_date_site_id_avg,
        site_installed_kw - LAG(site_installed_kw) OVER (PARTITION BY quality ORDER BY site_commissioned_on, id) AS site_installed_kw_change,
        RANK() OVER (PARTITION BY quality ORDER BY site_commissioned_on, id) AS site_commissioned_on_rank,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', read_at) ORDER BY read_at_day DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        COUNT(*) OVER (PARTITION BY TO_DATE(read_at), site_id) AS rows_in_read_at_to_date_site_id,
        ROUND(AVG(kwh) OVER (PARTITION BY YEAR(site_commissioned_on), site_id), 2) AS site_commissioned_on_year_site_id_avg_kwh
    FROM with_parent_site
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        quality,
        site_name,
        site_installed_kw,
        kwh_max_3,
        kwh_sum_7,
        read_at_cume_dist,
        site_commissioned_on_dense_rank,
        site_commissioned_on_dense_rank_2,
        site_installed_kw_vs_read_at_to_date_site_id_avg,
        site_installed_kw_change,
        site_commissioned_on_rank,
        first_known_kwh,
        rows_in_read_at_to_date_site_id,
        site_commissioned_on_year_site_id_avg_kwh,
        COALESCE(first_known_kwh, 0.0) AS first_known_kwh_filled,
        ROUND(kwh_sum_7 * 0.25, 2) AS kwh_sum_7_adj,
        TO_DATE(read_at_day) AS read_at_day_date
    FROM with_trends
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        quality,
        site_name,
        site_installed_kw,
        kwh_max_3,
        kwh_sum_7,
        read_at_cume_dist,
        site_commissioned_on_dense_rank,
        site_commissioned_on_dense_rank_2,
        site_installed_kw_vs_read_at_to_date_site_id_avg,
        site_commissioned_on_rank,
        first_known_kwh,
        rows_in_read_at_to_date_site_id,
        site_commissioned_on_year_site_id_avg_kwh,
        first_known_kwh_filled,
        kwh_sum_7_adj,
        read_at_day_date,
        ROW_NUMBER() OVER w_quality_site_commissioned_on AS kwh_sum_7_adj_seq,
        DENSE_RANK() OVER (PARTITION BY quality, site_commissioned_on_dense_rank_2 ORDER BY kwh_sum_7_adj DESC) AS kwh_sum_7_adj_dense_rank,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY quality, site_commissioned_on_dense_rank_2 ORDER BY kwh_sum_7_adj DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        ROUND(CUME_DIST() OVER (PARTITION BY site_commissioned_on_dense_rank_2, site_commissioned_on_dense_rank ORDER BY rows_in_read_at_to_date_site_id ASC), 3) AS rows_in_read_at_to_date_site_id_cume_dist,
        ROUND(AVG(site_commissioned_on_rank) OVER (PARTITION BY quality, site_commissioned_on_dense_rank_2 ORDER BY kwh_sum_7_adj DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS site_commissioned_on_rank_avg_7,
        ROUND(AVG(first_known_kwh_filled) OVER (PARTITION BY site_commissioned_on_dense_rank_2, site_commissioned_on_dense_rank ORDER BY rows_in_read_at_to_date_site_id ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS first_known_kwh_filled_avg_2_prior
    FROM tagged
    WINDOW w_quality_site_commissioned_on AS (PARTITION BY quality, site_commissioned_on_dense_rank_2 ORDER BY kwh_sum_7_adj DESC, id)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        site_commissioned_on_dense_rank_2,
        site_commissioned_on_dense_rank,
        DATE_TRUNC('MONTH', second_read_at) AS second_read_at_month,
        ROUND(SUM(first_known_kwh) / NULLIF(SUM(first_known_kwh_filled_avg_2_prior), 0), 3) AS first_known_kwh_to_first_known_kwh_filled_avg_2_,
        MAX(first_known_kwh) AS max_first_known_kwh
    FROM windowed
    GROUP BY site_commissioned_on_dense_rank_2, site_commissioned_on_dense_rank, DATE_TRUNC('MONTH', second_read_at)
)
-- Final projection
SELECT
    site_commissioned_on_dense_rank_2,
    site_commissioned_on_dense_rank,
    first_known_kwh_to_first_known_kwh_filled_avg_2_,
    DATE_TRUNC('MONTH', second_read_at_month) AS second_read_at_month_month,
    ABS(max_first_known_kwh - 84.0) AS max_first_known_kwh_distance
FROM totals
ORDER BY second_read_at_month_month NULLS LAST, max_first_known_kwh_distance DESC NULLS FIRST
