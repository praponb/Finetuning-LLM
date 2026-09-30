WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CASE WHEN voltage >= 236.7 THEN 'A' WHEN voltage >= 220.7 THEN 'B' ELSE 'C' END AS voltage_tier
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT s.*, si.commissioned_on AS site_commissioned_on, si.site_name
    FROM filtered s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(kwh) OVER w_site_commissioned_on_month AS site_commissioned_on_month_sum_kwh,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY YEAR(site_commissioned_on), DATE_TRUNC('MONTH', read_at) ORDER BY site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY MONTH(site_commissioned_on))) AS distinct_site_id_in_site_commissioned_on_month,
        DENSE_RANK() OVER (PARTITION BY site_id, voltage_tier ORDER BY kwh ASC, id) AS kwh_dense_rank,
        MIN(kwh) OVER (PARTITION BY MONTH(site_commissioned_on)) AS site_commissioned_on_month_min_kwh,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id, voltage_tier ORDER BY kwh ASC, id), 3) AS kwh_percent_rank
    FROM enriched_site
    WINDOW w_site_commissioned_on_month AS (PARTITION BY MONTH(site_commissioned_on))
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        SUM(kwh) AS total_reading_kwh,
        MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.total_reading_kwh,
        m.first_reading_read_at
    FROM with_trends s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        distinct_site_id_in_site_commissioned_on_month,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        COUNT(DISTINCT voltage_tier) AS distinct_voltage_tiers,
        SUM(distinct_site_id_in_site_commissioned_on_month) AS total_distinct_site_id_in_site_commissioned_on_m
    FROM summarized_rows
    WHERE site_commissioned_on_month_min_kwh < 780.0
    GROUP BY distinct_site_id_in_site_commissioned_on_month, DATE_TRUNC('MONTH', read_at)
)
SELECT
    distinct_site_id_in_site_commissioned_on_month,
    read_at_month,
    distinct_voltage_tiers
FROM rollup_base
ORDER BY distinct_voltage_tiers
