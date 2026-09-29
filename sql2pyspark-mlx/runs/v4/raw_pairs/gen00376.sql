WITH enriched AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        s.browser,
        s.device
    FROM web_events e
    LEFT JOIN sessions s
        ON e.session_id = s.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS row_seq,
        LAG(duration_sec, 1) OVER w_hist AS prev_duration_sec,
        LAG(duration_sec, 1) OVER (PARTITION BY device ORDER BY duration_sec, id) AS prev_duration_sec_2
    FROM enriched
    WINDOW w_hist AS (PARTITION BY page ORDER BY duration_sec, id)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY browser ORDER BY event_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM scored
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            session_id,
            browser,
            device,
            row_seq
        FROM islands
    ) src
    PIVOT (
        AVG(row_seq) AS total,
        COUNT(1) AS item_count
        FOR device IN ('tablet' AS tablet, 'mobile' AS mobile)
    )
)
SELECT
    s.session_id,
    s.browser,
    s.tablet_total,
    s.mobile_total,
    s.mobile_item_count,
    (SELECT MIN(t2.duration_sec) FROM web_events t2 WHERE t2.session_id = s.session_id) AS min_web_event_duration_sec
FROM pivoted s
ORDER BY session_id NULLS FIRST;
