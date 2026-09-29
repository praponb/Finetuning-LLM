WITH joined AS (
    SELECT src.*, p.name
    FROM sessions src
    LEFT JOIN customers p
        ON src.customer_id = p.id
)
SELECT
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    COUNT(CASE WHEN device = 'mobile' THEN 1 END) AS mobile_count,
    MAX(started_at) AS last_seen,
    COUNT_IF(country = 'SG') AS sg_events
FROM joined
GROUP BY activity_month
ORDER BY sg_events NULLS LAST
