WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        UPPER(TRIM(quality)) AS quality_clean,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix,
        CASE WHEN kwh >= 109.0 THEN 'large' WHEN kwh >= 34.1 THEN 'medium' ELSE 'small' END AS kwh_tier
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-02-18 12:00:00'
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_clean,
        s.kwh_tier,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone,
        si.tariff AS site_tariff
    FROM base s
    LEFT JOIN sites si ON si.id = s.site_id AND si.installed_kw > 141.6
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id, site_tariff), 2) AS voltage_vs_site_id_site_tariff_avg,
        COUNT(*) OVER (PARTITION BY site_id, site_tariff) AS rows_in_site_id_site_tariff,
        ROUND(PERCENT_RANK() OVER (ORDER BY read_at DESC NULLS FIRST), 3) AS read_at_percent_rank,
        DENSE_RANK() OVER (ORDER BY read_at DESC NULLS FIRST) AS read_at_dense_rank,
        SIZE(COLLECT_SET(kwh_tier) OVER (PARTITION BY site_id, site_tariff)) AS distinct_kwh_tier_in_site_id_site_tariff
    FROM with_parent_site
    WINDOW w_site_id_site_tariff AS (PARTITION BY site_id, site_tariff)
),
top_per_group AS (
    -- Top 3 per group
    SELECT *
    FROM metrics
    WHERE read_at_dense_rank <= 3
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(kwh_tier, 'ALL') AS kwh_tier,
    COALESCE(site_zone, 'ALL') AS site_zone,
    read_at_dense_rank,
    SUM(kwh) FILTER (WHERE quality NOT IN ('OK', 'EST')) AS kwh_quality_not_in_ok_est,
    MAX(read_at) AS last_read_at,
    SUM(read_at_dense_rank) FILTER (WHERE quality NOT IN ('BAD', 'EST')) AS read_at_dense_rank_quality_not_in_bad_est
FROM top_per_group
GROUP BY ROLLUP(kwh_tier, site_zone, read_at_dense_rank)
