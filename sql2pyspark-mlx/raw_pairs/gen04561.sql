WITH gaps AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
SELECT
    device,
    MAX(started_at) AS last_started_at,
    SUM(CASE WHEN device = 'tablet' THEN new_group_flag ELSE 0 END) AS tablet_new_group_flag,
    ROUND(SUM(session_id) / NULLIF(COUNT(CASE WHEN device = 'desktop' THEN 1 END), 0), 2) AS session_id_per_desktop
FROM sessionized
GROUP BY 1
ORDER BY tablet_new_group_flag DESC NULLS FIRST
