WITH filtered AS (
    SELECT *, CASE WHEN country IN ('JP', 'DE') THEN 'Tier 1' ELSE 'Other' END country_group
    FROM sessions
    WHERE started_at >= TO_DATE('2023-09-29')
      AND country IS NOT NULL
)
SELECT s.*
FROM filtered s
WHERE NOT EXISTS (
        SELECT 1
        FROM web_events ch
        WHERE ch.session_id = s.id
          AND ch.duration_sec <= 197
    )
