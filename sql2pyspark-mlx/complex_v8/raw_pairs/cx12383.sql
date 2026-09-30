WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id >= 7
      AND voltage >= 218.2
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY read_at DESC NULLS FIRST), 3) AS read_at_percent_rank,
        NTILE(10) OVER (PARTITION BY quality ORDER BY read_at, id) AS read_at_decile,
        COUNT(*) OVER w_read_at_year_site_id AS rows_in_read_at_year_site_id,
        SIZE(COLLECT_SET(quality) OVER w_read_at_year_site_id) AS distinct_quality_in_read_at_year_site_id,
        LEAD(kwh, 3) OVER (PARTITION BY site_id ORDER BY kwh, id) AS next3_kwh,
        NTILE(5) OVER (PARTITION BY MONTH(read_at) ORDER BY voltage DESC, id) AS voltage_quintile,
        ROUND(AVG(voltage) OVER (PARTITION BY MONTH(read_at) ORDER BY voltage DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_3,
        MIN(kwh) OVER (PARTITION BY quality) AS quality_min_kwh,
        SUM(kwh) OVER w_quality AS quality_sum_kwh,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY YEAR(read_at), site_id), 2) AS kwh_vs_read_at_year_site_id_avg,
        MAX(kwh) OVER (ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS kwh_max_4
    FROM base
    WINDOW w_read_at_year_site_id AS (PARTITION BY YEAR(read_at), site_id), w_quality AS (PARTITION BY quality)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(read_at_percent_rank / NULLIF(voltage_avg_3, 0), 3) AS read_at_percent_rank_per_voltage_avg_3
    FROM with_trends
    WHERE voltage_quintile > 5
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(read_at_percent_rank) OVER (PARTITION BY read_at_decile ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY voltage_quintile, site_id ORDER BY voltage_avg_3 DESC NULLS FIRST), 3) AS voltage_avg_3_cume_dist,
        MIN(quality_sum_kwh) OVER (PARTITION BY read_at_decile) AS read_at_decile_min_quality_sum_kwh,
        COUNT(*) OVER w_distinct_quality_in_read_at AS rows_in_distinct_quality_in_read_at_year_site_id,
        NTILE(4) OVER (ORDER BY rows_in_read_at_year_site_id, id) AS rows_in_read_at_year_site_id_quartile,
        LEAD(read_at, 2) OVER (PARTITION BY voltage_quintile, site_id ORDER BY voltage_avg_3 DESC NULLS FIRST, id) AS next2_read_at,
        ROUND(AVG(voltage) OVER (PARTITION BY voltage_quintile, site_id ORDER BY voltage_avg_3 DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_2
    FROM enriched
    WINDOW w_distinct_quality_in_read_at AS (PARTITION BY distinct_quality_in_read_at_year_site_id)
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    read_at_decile,
    rows_in_read_at_year_site_id,
    distinct_quality_in_read_at_year_site_id,
    voltage_quintile,
    quality_min_kwh,
    kwh_vs_read_at_year_site_id_avg,
    kwh_max_4,
    read_at_percent_rank_per_voltage_avg_3,
    last_read_at_percent_rank,
    read_at_decile_min_quality_sum_kwh,
    rows_in_distinct_quality_in_read_at_year_site_id,
    rows_in_read_at_year_site_id_quartile,
    voltage_avg_2,
    next2_read_at + INTERVAL 7 DAYS AS next2_read_at_plus_7d,
    next3_kwh + voltage_avg_3_cume_dist AS next3_kwh_plus_voltage_avg_3_cume_dist
FROM windowed
