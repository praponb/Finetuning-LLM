WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh BETWEEN 33.9 AND 117.4
      AND site_id IS NOT NULL
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        si.site_name
    FROM valid_readings s
    JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        site_name,
        RANK() OVER w_quality_site_id_by_read_at AS read_at_rank,
        MIN(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id) AS read_at_month_site_id_min_voltage,
        SUM(voltage) OVER w_site_id AS site_id_sum_voltage,
        MIN(voltage) OVER (PARTITION BY site_id) AS site_id_min_voltage,
        voltage - LAG(voltage) OVER (PARTITION BY site_id ORDER BY read_at DESC, id) AS voltage_change,
        ROUND(voltage / NULLIF(SUM(voltage) OVER w_read_at_month_site_id, 0), 4) AS voltage_share_of_read_at_month_site_id,
        SUM(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id) AS read_at_month_site_id_sum_voltage,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id), 2) AS voltage_vs_read_at_month_site_id_avg,
        SUM(voltage) OVER (ORDER BY voltage, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS voltage_sum_2_prior,
        ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY voltage DESC, id) AS seq_no,
        LAST_VALUE(read_at) OVER (ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at
    FROM combined_details
    WINDOW w_quality_site_id_by_read_at AS (PARTITION BY quality, site_id ORDER BY read_at, id), w_site_id AS (PARTITION BY site_id), w_read_at_month_site_id AS (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id)
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN voltage >= 231.7 THEN 'large' WHEN voltage >= 218.5 THEN 'medium' ELSE 'small' END AS voltage_tier,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM with_trends
    WHERE UPPER(TRIM(site_name)) LIKE 'S%'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(voltage_filled, 2) OVER (ORDER BY read_at_rank DESC, id) AS next2_voltage_filled,
        SUM(site_id_sum_voltage) OVER w_voltage_tier_site_id AS voltage_tier_site_id_sum_site_id_sum_voltage,
        ROUND(voltage_sum_2_prior - AVG(voltage_sum_2_prior) OVER (PARTITION BY MONTH(read_at), read_at_rank), 2) AS voltage_sum_2_prior_vs_read_at_month_read_at_ran,
        ROUND(AVG(voltage_filled) OVER w_voltage_tier_site_id, 2) AS voltage_tier_site_id_avg_voltage_filled,
        ROUND(read_at_rank / NULLIF(SUM(read_at_rank) OVER (PARTITION BY voltage_tier, site_id), 0), 4) AS read_at_rank_share_of_voltage_tier_site_id,
        LAST_VALUE(voltage_filled) IGNORE NULLS OVER (ORDER BY read_at_month_site_id_sum_voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage_filled
    FROM enriched
    WINDOW w_voltage_tier_site_id AS (PARTITION BY voltage_tier, site_id)
)
-- Final projection
SELECT
    id,
    site_id,
    voltage,
    site_name,
    read_at_rank,
    read_at_month_site_id_min_voltage,
    site_id_min_voltage,
    voltage_change,
    voltage_share_of_read_at_month_site_id,
    voltage_vs_read_at_month_site_id_avg,
    voltage_sum_2_prior,
    last_read_at,
    voltage_tier,
    voltage_filled,
    next2_voltage_filled,
    voltage_tier_site_id_sum_site_id_sum_voltage,
    voltage_tier_site_id_avg_voltage_filled,
    read_at_rank_share_of_voltage_tier_site_id,
    CONCAT(voltage_tier, '#', CAST(read_at_rank AS STRING)) AS voltage_tier_ref,
    DATEDIFF(DATE '2026-01-01', last_read_at) AS days_since_last_read_at
FROM metrics
ORDER BY voltage_tier DESC NULLS FIRST, id DESC
