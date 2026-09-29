WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(kwh * 1.1, 2) AS kwh_adj
    FROM meter_readings
    WHERE quality = 'OK'
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM recent_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_details_site AS (
    -- Join sites
    SELECT
        s.*,
        si.site_name,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on
    FROM sessionized s
    JOIN sites si ON si.id = s.site_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        prev_read_at,
        is_new_session,
        site_name,
        site_installed_kw,
        site_commissioned_on,
        ROUND(site_installed_kw / NULLIF(kwh, 0), 3) AS site_installed_kw_per_kwh,
        LENGTH(TRIM(site_name)) AS site_name_len
    FROM with_details_site
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at, site_id, quality, site_name_len
    FROM tagged
) src
PIVOT (
    SUM(site_name_len) AS total, MAX(site_name_len) AS max_v
    FOR quality IN ('OK', 'BAD')
)
ORDER BY read_at, site_id
