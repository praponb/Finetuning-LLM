WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE quality IS NOT NULL
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        voltage - LAG(voltage) OVER w_quality_by_read_at AS voltage_change,
        LAG(kwh, 1, 0) OVER w_quality_by_read_at AS prev_kwh
    FROM prepared_readings
    WINDOW w_quality_by_read_at AS (PARTITION BY quality ORDER BY read_at ASC, id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_group,
        COUNT(*) OVER w_quality_quality_group AS rows_in_quality_quality_group,
        ROUND(AVG(prev_kwh) OVER (PARTITION BY quality, quality_group), 2) AS quality_quality_group_avg_prev_kwh,
        LEAD(kwh, 3) OVER (ORDER BY voltage_change, id) AS next3_kwh
    FROM with_trends
    WINDOW w_quality_quality_group AS (PARTITION BY quality, quality_group)
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM windowed
    WHERE kwh < (SELECT MAX(kwh) FROM windowed)
),
joined_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_group,
        s.rows_in_quality_quality_group,
        s.quality_quality_group_avg_prev_kwh,
        s.next3_kwh,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM with_activity s
    JOIN sites si ON si.id = s.site_id
),
latest_per_key AS (
    -- Keep the latest row per site_zone
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_group,
        rows_in_quality_quality_group,
        quality_quality_group_avg_prev_kwh,
        next3_kwh,
        site_commissioned_on,
        site_zone
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_zone ORDER BY read_at DESC, id DESC) AS rn
        FROM joined_site
    ) ranked
    WHERE rn = 1
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        voltage,
        quality,
        rows_in_quality_quality_group,
        quality_quality_group_avg_prev_kwh,
        site_commissioned_on,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY read_at), 3) AS read_at_cume_dist,
        LEAD(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY read_at, id) AS next_voltage,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY read_at) AS read_at_dense_rank,
        LAG(site_commissioned_on, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY read_at, id) AS prev3_site_commissioned_on,
        voltage - LAG(voltage) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY read_at, id) AS voltage_change,
        next3_kwh - LAG(next3_kwh) OVER (PARTITION BY DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY read_at, id) AS next3_kwh_change
    FROM latest_per_key
)
SELECT
    id,
    site_id,
    voltage,
    quality,
    rows_in_quality_quality_group,
    quality_quality_group_avg_prev_kwh,
    site_commissioned_on,
    read_at_cume_dist,
    next_voltage,
    read_at_dense_rank,
    prev3_site_commissioned_on,
    next3_kwh_change,
    LENGTH(TRIM(quality)) AS quality_len,
    SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
FROM windowed_2
WHERE voltage BETWEEN 218.2 AND 241.8
ORDER BY site_id DESC NULLS FIRST
