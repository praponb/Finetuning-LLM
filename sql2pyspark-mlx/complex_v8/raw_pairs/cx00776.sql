WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
    WHERE kwh > 35.0
      AND quality IS NOT NULL
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(kwh) OVER (PARTITION BY site_id ORDER BY kwh_bucket ASC, id) AS prev_kwh,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY YEAR(read_at)), 0), 4) AS kwh_share_of_read_at_year,
        ROW_NUMBER() OVER (ORDER BY voltage ASC, id) AS rn,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY YEAR(read_at))) AS distinct_quality_in_read_at_year,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id ORDER BY kwh_bucket ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_1_prior
    FROM recent
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        prev_kwh,
        kwh_share_of_read_at_year,
        rn,
        distinct_quality_in_read_at_year,
        LAST_VALUE(kwh) OVER (ORDER BY prev_kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        COUNT(*) OVER (PARTITION BY rn, TO_DATE(read_at)) AS rows_in_rn_read_at_to_date,
        SUM(kwh_share_of_read_at_year) OVER (PARTITION BY rn, TO_DATE(read_at)) AS rn_read_at_to_date_sum_kwh_share_of_read_at_year,
        ROUND(voltage_avg_1_prior - AVG(voltage_avg_1_prior) OVER (PARTITION BY rn, TO_DATE(read_at)), 2) AS voltage_avg_1_prior_vs_rn_read_at_to_date_avg,
        FIRST_VALUE(kwh) OVER (PARTITION BY rn ORDER BY distinct_quality_in_read_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh
    FROM with_trends
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        quality,
        site_id,
        distinct_quality_in_read_at_year,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        ROUND(AVG(voltage_avg_1_prior_vs_rn_read_at_to_date_avg), 2) AS avg_voltage_avg_1_prior_vs_rn_read_at_to_date_av,
        SUM(CASE WHEN quality NOT IN ('OK', 'BAD') THEN 1 ELSE 0 END) AS n_quality_not_in_ok_bad,
        ROUND(AVG(prev_kwh), 2) AS avg_prev_kwh
    FROM metrics
    WHERE rn <= 7
    GROUP BY quality, site_id, distinct_quality_in_read_at_year
),
joined_site AS (
    -- Attach site attributes
    SELECT
        s.quality,
        s.site_id,
        s.distinct_quality_in_read_at_year,
        s.quality_list,
        s.avg_voltage_avg_1_prior_vs_rn_read_at_to_date_av,
        s.n_quality_not_in_ok_bad,
        s.avg_prev_kwh,
        si.installed_kw AS site_installed_kw
    FROM per_group s
    JOIN sites si ON si.id = s.site_id
),
combined_details_site AS (
    -- Attach site attributes
    SELECT
        s.quality,
        s.site_id,
        s.distinct_quality_in_read_at_year,
        s.quality_list,
        s.avg_voltage_avg_1_prior_vs_rn_read_at_to_date_av,
        s.n_quality_not_in_ok_bad,
        s.site_installed_kw,
        si.tariff AS site_tariff
    FROM joined_site s
    JOIN sites si ON si.id = s.site_id
)
-- Subtotals with CUBE
SELECT
    n_quality_not_in_ok_bad,
    distinct_quality_in_read_at_year,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list_2
FROM combined_details_site
GROUP BY CUBE(n_quality_not_in_ok_bad, distinct_quality_in_read_at_year)
ORDER BY n_quality_not_in_ok_bad NULLS LAST, distinct_quality_in_read_at_year NULLS LAST
