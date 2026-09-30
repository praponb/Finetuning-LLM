WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TO_DATE(started_at) AS started_at_date,
        COALESCE(country, 'N/A') AS resolved_country
    FROM sessions
    WHERE customer_id IS NOT NULL
      AND device NOT IN ('tablet', 'desktop')
)
-- Anti-join via a correlated subquery
SELECT s.*
FROM valid s
WHERE NOT EXISTS (
        SELECT 1
        FROM web_events x
        WHERE x.session_id = s.id
          AND x.event_time >= s.started_at_date
    );
