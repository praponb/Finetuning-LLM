WITH combined AS (
    -- Stack the branches
    SELECT id AS ref_id, commissioned_on AS event_date
    FROM sites
    WHERE zone = 'Z3'
      AND installed_kw BETWEEN 66.5 AND 373.3
    UNION ALL
    SELECT id, commissioned_on
    FROM sites
)
SELECT ref_id, COUNT(*) AS n_rows, MAX(event_date) AS latest_event_date
FROM combined
GROUP BY ref_id
ORDER BY ref_id
