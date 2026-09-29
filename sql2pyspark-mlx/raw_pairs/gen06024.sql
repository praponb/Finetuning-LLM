SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY event_time ASC, id ASC) AS seq_no,
    DENSE_RANK() OVER (PARTITION BY TO_DATE(event_time), page ORDER BY duration_sec DESC NULLS FIRST) AS duration_sec_dense_rank,
    NTILE(3) OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id ASC) AS duration_sec_bucket
FROM web_events
WHERE event_time < '2023-03-01';
