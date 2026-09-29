WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE quality IS NOT NULL
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(starts_session, 2) OVER (ORDER BY prev_read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_starts_session,
        ROW_NUMBER() OVER w_all_by_read_at AS seq_no,
        ROUND(starts_session / NULLIF(SUM(starts_session) OVER w_site_id_starts_session, 0), 4) AS starts_session_share_of_site_id_starts_session,
        DENSE_RANK() OVER (ORDER BY prev_read_at DESC) AS prev_read_at_dense_rank,
        SUM(burst_id) OVER w_site_id_starts_session AS site_id_starts_session_sum_burst_id,
        ROUND((UNIX_TIMESTAMP(prev_read_at) - UNIX_TIMESTAMP(LAG(prev_read_at) OVER (ORDER BY prev_read_at DESC, id))) / 60, 1) AS minutes_since_prev_prev_read_at
    FROM sessionized
    WINDOW w_all_by_read_at AS (ORDER BY read_at DESC, id), w_site_id_starts_session AS (PARTITION BY site_id, starts_session)
)
SELECT *
FROM (
    SELECT prev_read_at_dense_rank, prev_read_at, quality, second_starts_session
    FROM with_trends
) src
PIVOT (
    COUNT(second_starts_session) AS count_v, MAX(second_starts_session) AS max_v
    FOR quality IN ('BAD', 'EST', 'OK')
)
