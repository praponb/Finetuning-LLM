WITH stacked AS (
    -- Stack the branches
    SELECT status, start_date AS event_date
    FROM policies
    UNION ALL
    SELECT product AS status, end_date AS event_date
    FROM policies
    WHERE holder_id IS NOT NULL
      AND premium > 1051.9
)
SELECT status, COUNT(*) AS n_rows, MAX(event_date) AS latest_event_date
FROM stacked
GROUP BY status
ORDER BY status
