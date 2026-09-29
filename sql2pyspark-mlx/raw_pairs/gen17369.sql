WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM sessions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        device,
        started_at,
        country,
        session_no
    FROM islands
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country, customer_id ORDER BY is_new_session DESC NULLS FIRST, id) = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.duration_sec = 177
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        session_no,
        AVG(session_no) OVER (PARTITION BY country) AS country_avg_session_no,
        COUNT(session_no) OVER (PARTITION BY device) AS device_count_session_no
    FROM kept
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        country,
        device_count_session_no,
        CASE WHEN country = 'US' THEN 1 ELSE 0 END AS is_us,
        SUBSTRING(country, 1, 4) AS country_prefix,
        COALESCE(country, 'N/A') AS country_clean
    FROM with_windows
    WHERE country IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(is_us) OVER (PARTITION BY country ORDER BY device_count_session_no, id) AS prev_is_us,
        LAG(device_count_session_no) OVER (PARTITION BY country ORDER BY device_count_session_no, id) AS prev_device_count_session_no,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY device_count_session_no, id) AS seq_no
    FROM filtered
)
SELECT
    id,
    device_count_session_no,
    is_us,
    country_prefix,
    country_clean,
    prev_is_us,
    prev_device_count_session_no
FROM scored
ORDER BY prev_is_us;
