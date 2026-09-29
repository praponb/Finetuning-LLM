WITH matched_keys AS (
    SELECT event_type, id
    FROM web_events
    INTERSECT
    SELECT event_type, id
    FROM web_events
    WHERE event_time >= '2023-04-03'
),
verified AS (
    SELECT s.*
    FROM web_events s
    INNER JOIN matched_keys k ON s.event_type = k.event_type AND s.id = k.id
)
SELECT
    CASE WHEN duration_sec >= 526 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
    MIN(event_time) AS started_at,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    COUNT(DISTINCT id) AS distinct_ids
FROM verified
GROUP BY CASE WHEN duration_sec >= 526 THEN 'HIGH' ELSE 'LOW' END;
