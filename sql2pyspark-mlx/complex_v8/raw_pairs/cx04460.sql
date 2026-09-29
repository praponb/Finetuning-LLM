WITH prepared AS (
    -- Filter students
    SELECT *
    FROM students
),
all_events AS (
    -- Stack the branches
    SELECT id, enrolled_on AS event_date
    FROM prepared
    UNION
    SELECT id, enrolled_on AS event_date
    FROM students
)
SELECT id, COUNT(*) AS n_rows, MAX(event_date) AS latest_event_date
FROM all_events
GROUP BY id
ORDER BY id
