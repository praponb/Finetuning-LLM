WITH scoped_readings AS (
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
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM scoped_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 60 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(session_id) IGNORE NULLS OVER (ORDER BY prev_read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_session_id,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        SUM(session_id) OVER (ORDER BY prev_read_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS session_id_sum_1_prior
    FROM sessionized
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality,
    session_id,
    first_known_session_id,
    kwh_share_of_site_id,
    session_id_sum_1_prior
FROM ranked
ORDER BY quality DESC
