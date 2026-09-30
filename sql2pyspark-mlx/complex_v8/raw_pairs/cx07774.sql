WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.kwh) AS prior_kwh_1d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.prior_rows_1d,
        s.prior_kwh_1d,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff,
        si.site_name
    FROM in_window s
    JOIN sites si ON si.id = s.site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT SUM(m.voltage) FROM meter_readings m WHERE m.site_id = s.site_id) AS total_reading_voltage,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM with_details s
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        prior_kwh_1d,
        site_tariff,
        total_reading_voltage,
        reading_count,
        CASE WHEN site_tariff IN ('FLAT', 'EV') THEN 'group_a' WHEN site_tariff = 'TOU' THEN 'group_b' ELSE 'other' END AS site_tariff_group,
        CASE WHEN site_tariff IN ('TOU', 'EV') THEN 'group_a' WHEN site_tariff = 'FLAT' THEN 'group_b' ELSE 'other' END AS site_tariff_group_2
    FROM profiled
    WHERE site_installed_kw < 284.0
)
-- Final projection
SELECT
    id,
    site_id,
    prior_kwh_1d,
    site_tariff,
    reading_count,
    site_tariff_group,
    site_tariff_group_2,
    reading_count - prior_kwh_1d AS reading_count_minus_prior_kwh_1d,
    CONCAT(site_tariff, '#', CAST(site_id AS STRING)) AS site_tariff_ref
FROM labeled
