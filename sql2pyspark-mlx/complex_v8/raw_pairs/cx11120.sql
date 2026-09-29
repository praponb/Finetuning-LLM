WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN voltage >= 237.5 THEN 'large' WHEN voltage >= 223.3 THEN 'medium' ELSE 'small' END AS voltage_tier,
        CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-08 09:00:00'
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        read_at,
        kwh,
        quality,
        voltage_tier,
        CASE WHEN kwh >= 81.1 THEN 'HIGH' WHEN kwh >= 28.4 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier
    FROM scoped
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE kwh > (SELECT AVG(kwh) FROM tagged)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        kwh,
        quality,
        voltage_tier,
        SUM(kwh) OVER (PARTITION BY MONTH(read_at), kwh_tier) AS read_at_month_kwh_tier_sum_kwh,
        MAX(kwh) OVER (ORDER BY read_at ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS kwh_max_4,
        kwh - LAG(kwh) OVER w_all_by_read_at AS kwh_change,
        LEAD(read_at, 3) OVER (ORDER BY read_at ASC, id) AS next3_read_at,
        RANK() OVER (ORDER BY read_at ASC) AS read_at_rank,
        LAG(read_at, 3) OVER w_all_by_read_at AS prev3_read_at
    FROM with_activity
    WINDOW w_all_by_read_at AS (ORDER BY read_at ASC, id)
),
summary AS (
    -- Summarise per group
    SELECT
        voltage_tier,
        COUNT(*) FILTER (WHERE quality = 'BAD') AS n_quality_bad,
        COUNT(*) AS cnt,
        COUNT(DISTINCT voltage_tier) AS distinct_voltage_tiers
    FROM metrics
    GROUP BY voltage_tier
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN cnt >= 8 THEN 'HIGH' WHEN cnt >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS cnt_tier
    FROM summary
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM derived
    WHERE n_quality_bad < (SELECT MAX(n_quality_bad) FROM derived)
)
-- Final projection
SELECT
    voltage_tier,
    distinct_voltage_tiers,
    cnt_tier,
    CONCAT_WS('|', cnt_tier, voltage_tier) AS cnt_tier_voltage_tier_label
FROM screened
ORDER BY voltage_tier DESC
