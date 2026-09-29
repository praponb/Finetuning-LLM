WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN voltage >= 238.8 THEN 'HIGH' WHEN voltage >= 222.3 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_tier
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_group AS (
    -- Group-level totals
    SELECT
        voltage_tier,
        quality,
        YEAR(read_at) AS read_at_year,
        SUM(is_new_session) AS total_is_new_session,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        ROUND(AVG(session_id), 2) AS avg_session_id,
        ROUND(AVG(kwh), 2) AS avg_kwh
    FROM sessionized
    GROUP BY 1, 2, 3
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        voltage_tier,
        quality,
        read_at_year,
        total_is_new_session,
        quality_list,
        avg_kwh,
        ROUND(CUME_DIST() OVER w_quality_total_is_new_session, 3) AS avg_session_id_cume_dist,
        ROUND(AVG(avg_session_id) OVER (PARTITION BY quality, total_is_new_session ORDER BY avg_session_id NULLS LAST, voltage_tier, quality, read_at_year ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS avg_session_id_avg_6_prior,
        ROUND(avg_session_id - AVG(avg_session_id) OVER w_read_at_year_total_is_new_se, 2) AS avg_session_id_vs_read_at_year_total_is_new_sess,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY read_at_year, total_is_new_session)) AS distinct_quality_in_read_at_year_total_is_new_se,
        avg_session_id - LAG(avg_session_id) OVER (PARTITION BY total_is_new_session, voltage_tier ORDER BY avg_kwh, voltage_tier, quality, read_at_year) AS avg_session_id_change,
        SUM(avg_kwh) OVER (PARTITION BY total_is_new_session, voltage_tier ORDER BY avg_kwh, voltage_tier, quality, read_at_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_kwh
    FROM per_group
    WINDOW w_quality_total_is_new_session AS (PARTITION BY quality, total_is_new_session ORDER BY avg_session_id NULLS LAST), w_read_at_year_total_is_new_se AS (PARTITION BY read_at_year, total_is_new_session)
)
SELECT
    voltage_tier,
    read_at_year,
    total_is_new_session,
    avg_kwh,
    avg_session_id_cume_dist,
    avg_session_id_avg_6_prior,
    avg_session_id_vs_read_at_year_total_is_new_sess,
    distinct_quality_in_read_at_year_total_is_new_se,
    avg_session_id_change,
    running_avg_kwh,
    ROUND(running_avg_kwh * 0.9, 2) AS running_avg_kwh_adj
FROM ranked
WHERE quality = 'OK'
