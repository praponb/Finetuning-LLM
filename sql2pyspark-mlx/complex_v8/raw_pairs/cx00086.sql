WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('OK', 'BAD') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group,
        DATE_TRUNC('YEAR', read_at) AS read_at_year
    FROM meter_readings
    WHERE voltage >= 222.6
      AND kwh > 20.4
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(voltage) OVER (PARTITION BY quality ORDER BY kwh ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_7,
        ROW_NUMBER() OVER (PARTITION BY quality ORDER BY kwh ASC, id) AS rn,
        SIZE(COLLECT_SET(quality_group) OVER (PARTITION BY site_id)) AS distinct_quality_group_in_site_id,
        COUNT(*) OVER w_quality_group_quality AS rows_in_quality_group_quality,
        MAX(voltage) OVER (PARTITION BY quality_group, quality) AS quality_group_quality_max_voltage,
        RANK() OVER w_all_by_kwh_ties AS kwh_rank,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality, quality_group)) AS distinct_site_id_in_quality_quality_group,
        LAST_VALUE(read_at_year) OVER (PARTITION BY quality_group ORDER BY read_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at_year,
        COUNT(*) OVER (PARTITION BY quality_group ORDER BY read_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM prepared
    WINDOW w_quality_group_quality AS (PARTITION BY quality_group, quality), w_all_by_kwh_ties AS (ORDER BY kwh ASC)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE kwh_rank = 1
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(10) OVER (ORDER BY distinct_site_id_in_quality_quality_group DESC, id) AS distinct_site_id_in_quality_quality_group_decile,
        SUM(voltage_avg_7) OVER (PARTITION BY distinct_quality_group_in_site_id ORDER BY voltage, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS voltage_avg_7_sum_2_prior,
        LAST_VALUE(kwh_rank) IGNORE NULLS OVER (PARTITION BY quality_group, kwh_rank ORDER BY distinct_quality_group_in_site_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh_rank,
        MAX(rn) OVER (PARTITION BY quality_group, kwh_rank ORDER BY distinct_quality_group_in_site_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_rn_so_far,
        LAG(last_read_at_year) OVER (ORDER BY distinct_site_id_in_quality_quality_group DESC, id) AS prev_last_read_at_year,
        MAX(kwh) OVER (PARTITION BY distinct_quality_group_in_site_id ORDER BY voltage, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS kwh_max_6_prior
    FROM leaders
)
SELECT
    id,
    site_id,
    read_at,
    quality,
    quality_group,
    voltage_avg_7,
    rn,
    distinct_quality_group_in_site_id,
    rows_in_quality_group_quality,
    kwh_rank,
    distinct_site_id_in_quality_quality_group,
    last_read_at_year,
    running_count,
    last_known_kwh_rank,
    prev_last_read_at_year,
    kwh_max_6_prior,
    ROUND(rn / NULLIF(voltage, 0), 3) AS rn_per_voltage
FROM windowed
ORDER BY quality_group DESC, site_id DESC
