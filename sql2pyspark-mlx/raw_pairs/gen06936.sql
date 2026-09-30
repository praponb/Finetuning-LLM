WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        ROW_NUMBER() OVER (PARTITION BY YEAR(event_time), session_id ORDER BY event_time DESC, id) AS seq_no,
        AVG(duration_sec) OVER (PARTITION BY session_id, event_type ORDER BY event_time, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS duration_sec_avg_5,
        RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', event_time), event_type ORDER BY duration_sec DESC) AS duration_sec_rank
    FROM web_events
)
SELECT
    id,
    event_type,
    page,
    duration_sec,
    seq_no,
    duration_sec_avg_5,
    duration_sec_rank
FROM metrics;
