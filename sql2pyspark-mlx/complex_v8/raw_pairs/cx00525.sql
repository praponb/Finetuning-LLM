WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh <= 111.9
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER w_quality_site_id_by_kwh_ties AS kwh_rank,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at
    FROM scoped_readings
    WINDOW w_quality_site_id_by_kwh_ties AS (PARTITION BY quality, site_id ORDER BY kwh NULLS LAST)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(kwh_rank, kwh) AS least_kwh_rank_kwh,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM metrics
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.kwh_rank,
        s.least_kwh_rank_kwh,
        s.voltage_filled,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM annotated s
    JOIN sites si ON si.id = s.site_id
),
with_parent AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.kwh_rank,
        s.least_kwh_rank_kwh,
        s.voltage_filled,
        s.site_tariff,
        s.site_commissioned_on,
        s.site_installed_kw,
        si.zone AS site_zone
    FROM joined_site s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone <> 'Z3'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(kwh_rank) OVER (ORDER BY least_kwh_rank_kwh, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS kwh_rank_avg_7,
        NTH_VALUE(site_installed_kw, 2) OVER (ORDER BY least_kwh_rank_kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_site_installed_kw
    FROM with_parent
)
SELECT
    site_id,
    kwh_rank,
    quality,
    ROUND(STDDEV(second_site_installed_kw), 2) AS stddev_second_site_installed_kw,
    COUNT(*) AS cnt,
    SUM(CASE WHEN site_zone IS NOT NULL THEN kwh_rank_avg_7 ELSE 0 END) AS kwh_rank_avg_7_site_zone_is_not_null
FROM sequenced
GROUP BY CUBE(site_id, kwh_rank, quality)
ORDER BY site_id NULLS LAST, kwh_rank NULLS LAST, quality NULLS LAST
