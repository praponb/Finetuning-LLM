WITH prepared AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        tariff,
        installed_kw,
        CASE WHEN installed_kw >= 386.6 THEN 'A' WHEN installed_kw >= 141.6 THEN 'B' ELSE 'C' END AS installed_kw_tier,
        DAYOFWEEK(commissioned_on) AS commissioned_on_dayofweek
    FROM sites
    WHERE (tariff IN ('TOU', 'EV') OR commissioned_on < DATE '2025-05-07')
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(installed_kw_tier), ' ', '_')) AS installed_kw_tier_slug
    FROM prepared
    WHERE commissioned_on_dayofweek > 2
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        ROUND(AVG(kwh), 2) AS avg_reading_kwh,
        MAX(read_at) AS last_reading_read_at,
        MIN(voltage) AS min_reading_voltage
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.avg_reading_kwh,
        m.last_reading_read_at,
        m.min_reading_voltage
    FROM derived s
    LEFT JOIN reading_stats m ON m.site_id = s.id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY installed_kw_tier, tariff ORDER BY avg_reading_kwh DESC, id) AS rn,
        ROUND(PERCENT_RANK() OVER w_installed_kw_tier_tariff_by, 3) AS avg_reading_kwh_percent_rank
    FROM summarized_rows
    WINDOW w_installed_kw_tier_tariff_by AS (PARTITION BY installed_kw_tier, tariff ORDER BY avg_reading_kwh DESC, id)
)
SELECT *
FROM (
    SELECT rn, tariff, commissioned_on_dayofweek
    FROM sequenced
) src
PIVOT (
    AVG(commissioned_on_dayofweek)
    FOR tariff IN ('TOU', 'FLAT', 'EV')
)
ORDER BY rn
