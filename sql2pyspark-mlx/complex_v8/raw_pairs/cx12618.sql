WITH filtered_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        SUBSTRING(TRIM(priority), 1, 4) AS priority_prefix
    FROM tickets
    WHERE MONTH(opened_at) IN (1, 3)
      AND csat BETWEEN 2 AND 5
),
summary AS (
    -- Summarise per group
    SELECT
        TO_DATE(closed_at) AS closed_at_day,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        MAX(csat) AS max_csat,
        PERCENTILE_APPROX(csat, 0.5) AS median_csat,
        SUM(CASE WHEN priority IS NOT NULL THEN csat ELSE 0 END) AS csat_priority_is_not_null,
        SUM(csat) AS total_csat,
        ROUND(SUM(csat) / NULLIF(SUM(requester_id), 0), 3) AS csat_to_requester_id_ratio
    FROM filtered_tickets
    GROUP BY TO_DATE(closed_at), DATE_TRUNC('MONTH', opened_at)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', opened_at_month) AS days_since_opened_at_month
    FROM summary
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM annotated
    WHERE csat_to_requester_id_ratio > (SELECT AVG(csat_to_requester_id_ratio) FROM annotated)
),
with_activity_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_activity
    WHERE csat_priority_is_not_null > (SELECT AVG(csat_priority_is_not_null) FROM with_activity)
)
SELECT
    closed_at_day,
    opened_at_month,
    max_csat,
    median_csat,
    csat_priority_is_not_null,
    csat_to_requester_id_ratio,
    days_since_opened_at_month,
    CASE WHEN total_csat >= 18 THEN 'A' WHEN total_csat >= 10 THEN 'B' ELSE 'C' END AS total_csat_tier
FROM with_activity_2
WHERE days_since_opened_at_month < 525
ORDER BY closed_at_day, median_csat DESC
