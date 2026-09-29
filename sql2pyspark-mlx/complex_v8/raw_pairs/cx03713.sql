WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('MONTH', read_at) AS read_at_month
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT s.*, si.tariff AS site_tariff
    FROM sessionized s
    JOIN sites si ON si.id = s.site_id
),
joined AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.read_at_month,
        s.starts_session,
        s.burst_id,
        s.site_tariff,
        si.site_name
    FROM combined_details_site s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT id, metric_name, amount
FROM (
    SELECT id, starts_session, burst_id
    FROM joined
) src
UNPIVOT (
    amount FOR metric_name IN (starts_session, burst_id)
) unp
ORDER BY id, metric_name
