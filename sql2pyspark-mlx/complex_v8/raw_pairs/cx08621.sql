WITH stacked AS (
    -- Stack the branches
    SELECT id AS key_id, signup_date AS event_date, 'listener' AS record_type
    FROM listeners
    WHERE plan IS NOT NULL
      AND signup_date >= DATE '2024-08-02'
    UNION
    SELECT id AS key_id, release_date AS event_date, 'track' AS record_type
    FROM tracks
)
SELECT record_type, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id, MAX(event_date) AS latest_event_date
FROM stacked
GROUP BY record_type
ORDER BY record_type
