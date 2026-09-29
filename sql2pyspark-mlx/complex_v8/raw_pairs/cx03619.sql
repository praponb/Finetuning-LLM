WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE kwh <= 90.6
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.*,
        si.site_name,
        si.commissioned_on AS site_commissioned_on,
        si.tariff AS site_tariff
    FROM prepared_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER w_site_id_quality AS rows_in_site_id_quality,
        ROUND(AVG(voltage) OVER (ORDER BY site_commissioned_on DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_2_prior,
        SIZE(COLLECT_SET(site_tariff) OVER (PARTITION BY site_id, quality)) AS distinct_site_tariff_in_site_id_quality,
        ROUND(PERCENT_RANK() OVER w_site_id_site_tariff_by_kwh_t, 3) AS kwh_percent_rank,
        SUM(kwh) OVER (PARTITION BY site_tariff ORDER BY kwh DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS kwh_sum_2,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY quality, site_tariff ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage,
        SIZE(COLLECT_SET(site_tariff) OVER w_site_id) AS distinct_site_tariff_in_site_id,
        MAX(kwh_filled) OVER (PARTITION BY quality, site_tariff ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS kwh_filled_max_1_prior,
        ROW_NUMBER() OVER (PARTITION BY quality ORDER BY kwh_filled ASC, id) AS kwh_filled_seq,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id, quality), 2) AS site_id_quality_avg_kwh,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id), 2) AS site_id_avg_voltage,
        MAX(voltage) OVER w_site_id_site_commissioned_on AS site_id_site_commissioned_on_year_max_voltage
    FROM with_details
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality), w_site_id_site_tariff_by_kwh_t AS (PARTITION BY site_id, site_tariff ORDER BY kwh), w_site_id AS (PARTITION BY site_id), w_site_id_site_commissioned_on AS (PARTITION BY site_id, YEAR(site_commissioned_on))
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(kwh_filled_seq, 2, 0) OVER (ORDER BY kwh_percent_rank, id) AS next2_kwh_filled_seq,
        ROUND(AVG(first_known_voltage) OVER (PARTITION BY quality, site_tariff), 2) AS quality_site_tariff_avg_first_known_voltage,
        SUM(kwh_filled_max_1_prior) OVER (ORDER BY kwh_percent_rank, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS kwh_filled_max_1_prior_sum_2,
        LAST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY kwh_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh
    FROM windowed
)
SELECT
    id,
    site_id,
    kwh,
    voltage,
    kwh_filled,
    site_name,
    site_commissioned_on,
    site_tariff,
    voltage_avg_2_prior,
    distinct_site_tariff_in_site_id_quality,
    first_known_voltage,
    kwh_filled_max_1_prior,
    site_id_quality_avg_kwh,
    site_id_avg_voltage,
    site_id_site_commissioned_on_year_max_voltage,
    next2_kwh_filled_seq,
    quality_site_tariff_avg_first_known_voltage,
    kwh_filled_max_1_prior_sum_2,
    last_known_kwh,
    LOWER(REGEXP_REPLACE(TRIM(site_tariff), ' ', '_')) AS site_tariff_slug,
    DATEDIFF(DATE '2025-12-31', site_commissioned_on) AS days_since_site_commissioned_on
FROM sequenced
ORDER BY site_id_quality_avg_kwh DESC, site_tariff
