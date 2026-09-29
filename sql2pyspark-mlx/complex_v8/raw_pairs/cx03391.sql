WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        UPPER(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE MONTH(read_at) IN (2, 3)
      AND voltage < 233.2
),
enriched AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality_clean,
        si.commissioned_on AS site_commissioned_on
    FROM base_readings s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on >= DATE '2024-08-24'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        kwh,
        site_commissioned_on,
        NTILE(4) OVER w_site_commissioned_on_month_b AS kwh_quartile,
        FIRST_VALUE(site_commissioned_on) IGNORE NULLS OVER (PARTITION BY MONTH(site_commissioned_on) ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_site_commissioned_on,
        SUM(kwh) OVER w_site_id AS site_id_sum_kwh,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), site_id ORDER BY kwh DESC, id) AS kwh_tercile,
        DENSE_RANK() OVER w_site_commissioned_on_month_b AS kwh_dense_rank,
        SUM(kwh) OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY site_commissioned_on DESC NULLS FIRST), 3) AS site_commissioned_on_cume_dist,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id ORDER BY site_commissioned_on DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_2_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY site_commissioned_on DESC NULLS FIRST, id), 3) AS site_commissioned_on_cume_dist_2,
        MAX(kwh) OVER (PARTITION BY site_id) AS site_id_max_kwh,
        ROUND(CUME_DIST() OVER w_site_commissioned_on_month_s_2, 3) AS kwh_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY kwh DESC), 3) AS kwh_percent_rank,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY MONTH(site_commissioned_on)), 2) AS kwh_vs_site_commissioned_on_month_avg,
        NTILE(3) OVER (PARTITION BY site_id ORDER BY site_commissioned_on DESC NULLS FIRST, id) AS site_commissioned_on_tercile,
        MIN(kwh) OVER (PARTITION BY MONTH(site_commissioned_on) ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_so_far
    FROM enriched
    WINDOW w_site_commissioned_on_month_b AS (PARTITION BY MONTH(site_commissioned_on) ORDER BY kwh DESC, id), w_site_id AS (PARTITION BY site_id), w_site_commissioned_on_month_s_2 AS (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on), site_id ORDER BY kwh DESC)
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        kwh,
        site_commissioned_on,
        kwh_quartile,
        first_known_site_commissioned_on,
        site_id_sum_kwh,
        kwh_tercile,
        running_kwh,
        site_commissioned_on_cume_dist,
        kwh_avg_2_prior,
        site_commissioned_on_cume_dist_2,
        site_id_max_kwh,
        kwh_cume_dist,
        kwh_percent_rank,
        kwh_vs_site_commissioned_on_month_avg,
        site_commissioned_on_tercile,
        min_kwh_so_far
    FROM ranked
    WHERE kwh_dense_rank <= 3
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(site_id_sum_kwh) OVER (PARTITION BY kwh_tercile, kwh_quartile), 2) AS kwh_tercile_kwh_quartile_avg_site_id_sum_kwh,
        RANK() OVER (PARTITION BY kwh_tercile ORDER BY kwh ASC) AS kwh_rank,
        DENSE_RANK() OVER (PARTITION BY kwh_tercile ORDER BY kwh ASC) AS kwh_dense_rank
    FROM first_rows
)
SELECT
    id,
    kwh,
    site_commissioned_on,
    kwh_quartile,
    site_id_sum_kwh,
    kwh_tercile,
    running_kwh,
    site_commissioned_on_cume_dist,
    kwh_avg_2_prior,
    site_id_max_kwh,
    kwh_cume_dist,
    kwh_percent_rank,
    kwh_vs_site_commissioned_on_month_avg,
    site_commissioned_on_tercile,
    kwh_tercile_kwh_quartile_avg_site_id_sum_kwh,
    kwh_rank,
    kwh_dense_rank,
    ABS(kwh_cume_dist - 0.7) AS kwh_cume_dist_distance
FROM metrics
ORDER BY site_id_sum_kwh DESC
