WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        TO_DATE(read_at) AS read_at_date
    FROM meter_readings
    WHERE kwh BETWEEN 45.0 AND 83.8
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 45 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(burst_id) OVER w_all_by_starts_session AS next_burst_id,
        LAG(prev_read_at) OVER w_quality_by_prev_read_at AS prev_prev_read_at,
        DENSE_RANK() OVER (PARTITION BY quality ORDER BY prev_read_at DESC) AS prev_read_at_dense_rank,
        MIN(burst_id) OVER (PARTITION BY MONTH(prev_read_at), site_id) AS prev_read_at_month_site_id_min_burst_id
    FROM sessionized
    WINDOW w_all_by_starts_session AS (ORDER BY starts_session ASC NULLS LAST, id), w_quality_by_prev_read_at AS (PARTITION BY quality ORDER BY prev_read_at DESC, id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at, quality, kwh
    FROM sequenced
) src
PIVOT (
    SUM(kwh)
    FOR quality IN ('BAD', 'EST', 'OK')
)
ORDER BY read_at
