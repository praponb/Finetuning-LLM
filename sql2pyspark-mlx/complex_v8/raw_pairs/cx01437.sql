WITH prepared AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality = 'BAD'
      AND site_id IS NOT NULL
),
joined AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        si.tariff AS site_tariff
    FROM prepared s
    JOIN sites si ON si.id = s.site_id AND si.site_name IS NOT NULL
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        site_tariff,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY YEAR(read_at))) AS distinct_quality_in_read_at_year,
        RANK() OVER (ORDER BY kwh, id) AS kwh_rank,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY read_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        ROUND(PERCENT_RANK() OVER w_read_at_year_by_read_at, 3) AS read_at_percent_rank,
        LAG(kwh) OVER (ORDER BY kwh, id) AS prev_kwh,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY YEAR(read_at)), 0), 4) AS kwh_share_of_read_at_year,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY YEAR(read_at))) AS distinct_site_id_in_read_at_year,
        COUNT(*) OVER (PARTITION BY YEAR(read_at)) AS rows_in_read_at_year,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_all_by_read_at)) / 60, 1) AS minutes_since_prev_read_at,
        NTILE(10) OVER w_read_at_year_by_read_at AS read_at_decile,
        SIZE(COLLECT_SET(site_tariff) OVER (PARTITION BY YEAR(read_at))) AS distinct_site_tariff_in_read_at_year,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY YEAR(read_at)), 2) AS kwh_vs_read_at_year_avg,
        LEAD(read_at) OVER w_all_by_kwh AS next_read_at,
        ROUND(AVG(kwh) OVER w_read_at_year, 2) AS read_at_year_avg_kwh,
        MAX(kwh) OVER (PARTITION BY YEAR(read_at) ORDER BY read_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS kwh_max_7
    FROM joined
    WINDOW w_read_at_year_by_read_at AS (PARTITION BY YEAR(read_at) ORDER BY read_at DESC, id), w_all_by_read_at AS (ORDER BY read_at ASC NULLS LAST, id), w_all_by_kwh AS (ORDER BY kwh, id), w_read_at_year AS (PARTITION BY YEAR(read_at))
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM sequenced
    WHERE kwh_rank <= 2
      AND distinct_site_tariff_in_read_at_year BETWEEN 4 AND 9
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    site_tariff,
    distinct_quality_in_read_at_year,
    kwh_rank,
    read_at_percent_rank,
    prev_kwh,
    kwh_share_of_read_at_year,
    distinct_site_id_in_read_at_year,
    rows_in_read_at_year,
    minutes_since_prev_read_at,
    kwh_vs_read_at_year_avg,
    next_read_at,
    read_at_year_avg_kwh,
    kwh_max_7,
    CASE WHEN site_tariff IN ('TOU', 'FLAT') THEN 'group_a' WHEN site_tariff = 'EV' THEN 'group_b' ELSE 'other' END AS site_tariff_group,
    CONCAT(site_tariff, '#', CAST(read_at_decile AS STRING)) AS site_tariff_ref
FROM first_rows
ORDER BY kwh_max_7, site_tariff_group DESC
