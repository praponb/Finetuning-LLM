WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        si.commissioned_on AS site_commissioned_on,
        si.site_name
    FROM base s
    JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        LAG(site_commissioned_on) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY voltage DESC, id) AS prev_site_commissioned_on,
        SUM(voltage) OVER (PARTITION BY site_id) AS site_id_sum_voltage,
        ROW_NUMBER() OVER w_site_id_site_commissioned_on AS rn,
        MAX(kwh) OVER (PARTITION BY site_id ORDER BY site_commissioned_on DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS kwh_max_2_prior,
        LEAD(site_commissioned_on, 2) OVER (PARTITION BY site_id ORDER BY site_commissioned_on DESC, id) AS next2_site_commissioned_on,
        voltage - LAG(voltage) OVER w_site_id_by_site_commissioned_2 AS voltage_change,
        ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY site_commissioned_on DESC, id) AS rn_2,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_site_id, 0), 4) AS voltage_share_of_site_id,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id), 2) AS site_id_avg_voltage,
        LEAD(voltage) OVER w_all_by_voltage AS next_voltage,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY site_id), 2) AS voltage_vs_site_id_avg
    FROM enriched
    WINDOW w_site_id_site_commissioned_on AS (PARTITION BY site_id, DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY voltage DESC, id), w_site_id_by_site_commissioned_2 AS (PARTITION BY site_id ORDER BY site_commissioned_on DESC, id), w_site_id AS (PARTITION BY site_id), w_all_by_voltage AS (ORDER BY voltage DESC, id)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        site_commissioned_on,
        site_name,
        prev_site_commissioned_on,
        site_id_sum_voltage,
        kwh_max_2_prior,
        next2_site_commissioned_on,
        voltage_change,
        voltage_share_of_site_id,
        site_id_avg_voltage,
        voltage_vs_site_id_avg,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        DATE_FORMAT(site_commissioned_on, 'yyyy-MM') AS site_commissioned_on_label
    FROM windowed
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', site_commissioned_on) AS site_commissioned_on_month,
        site_id,
        quality,
        ROUND(SUM(site_id_sum_voltage) / NULLIF(SUM(kwh), 0), 3) AS site_id_sum_voltage_to_kwh_ratio,
        MIN(site_commissioned_on) AS first_site_commissioned_on,
        ROUND(100 * COUNT(CASE WHEN LOWER(site_name) LIKE '% b%' THEN 1 END) / COUNT(*), 1) AS pct_lower_site_name_like_b,
        SUM(site_id_sum_voltage) AS total_site_id_sum_voltage
    FROM tagged
    GROUP BY DATE_TRUNC('MONTH', site_commissioned_on), site_id, quality
    HAVING SUM(voltage_share_of_site_id) > 0.1
)
-- Final projection
SELECT
    site_commissioned_on_month,
    site_id,
    quality,
    site_id_sum_voltage_to_kwh_ratio,
    total_site_id_sum_voltage,
    UPPER(TRIM(quality)) AS quality_clean
FROM rollup_base
WHERE quality NOT IN ('EST', 'OK')
ORDER BY quality DESC NULLS FIRST
