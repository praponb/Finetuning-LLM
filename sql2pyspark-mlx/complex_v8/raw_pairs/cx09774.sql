WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE site_id <= 3
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        kwh,
        MAX(voltage) OVER (ORDER BY voltage, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS voltage_max_2_prior,
        NTH_VALUE(voltage, 2) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        ROUND(CUME_DIST() OVER (ORDER BY voltage), 3) AS voltage_cume_dist,
        SUM(kwh) OVER (PARTITION BY site_id, YEAR(read_at) ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh
    FROM scoped
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(running_kwh, voltage_max_2_prior) AS greatest_running_kwh_voltage_max_2_prior,
        COALESCE(running_kwh, 0.0) AS running_kwh_filled,
        ABS(second_voltage - 227.1) AS second_voltage_distance
    FROM windowed
    WHERE kwh <= 113.1
),
with_parent AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage_max_2_prior,
        s.second_voltage,
        s.running_kwh,
        s.greatest_running_kwh_voltage_max_2_prior,
        s.running_kwh_filled,
        s.second_voltage_distance,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM scored s
    JOIN sites si ON si.id = s.site_id
),
combined_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.voltage_max_2_prior,
        s.second_voltage,
        s.running_kwh,
        s.second_voltage_distance,
        s.site_installed_kw,
        si.installed_kw AS site_installed_kw_2
    FROM with_parent s
    JOIN sites si ON si.id = s.site_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN voltage_max_2_prior >= 630.0 THEN 'HIGH' WHEN voltage_max_2_prior >= 446.2 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_max_2_prior_tier
    FROM combined_details
    WHERE voltage_max_2_prior >= 315.0
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_voltage_max_2_prior_tier_sit AS rows_in_voltage_max_2_prior_tier_site_id,
        ROUND(AVG(second_voltage_distance) OVER (ORDER BY running_kwh, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS second_voltage_distance_avg_2_prior,
        FIRST_VALUE(site_installed_kw) IGNORE NULLS OVER (ORDER BY running_kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_site_installed_kw,
        ROUND(second_voltage - AVG(second_voltage) OVER (PARTITION BY voltage_max_2_prior_tier, site_id), 2) AS second_voltage_vs_voltage_max_2_prior_tier_site_,
        NTH_VALUE(voltage_max_2_prior, 2) OVER (ORDER BY running_kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage_max_2_prior
    FROM annotated
    WINDOW w_voltage_max_2_prior_tier_sit AS (PARTITION BY voltage_max_2_prior_tier, site_id)
)
-- Subtotals with GROUPING SETS
SELECT
    site_id,
    voltage_max_2_prior_tier,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT voltage_max_2_prior_tier) AS distinct_voltage_max_2_prior_tiers,
    ROUND(SUM(second_voltage), 2) AS total_second_voltage
FROM with_windows
GROUP BY GROUPING SETS ((site_id, voltage_max_2_prior_tier), (site_id))
