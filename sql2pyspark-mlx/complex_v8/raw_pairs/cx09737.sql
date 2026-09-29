WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
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
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        GREATEST(requester_id, csat) AS greatest_requester_id_csat
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 6 AND 11
      AND MONTH(opened_at) IN (1, 2)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        opened_at + INTERVAL 30 MINUTES AS opened_at_plus_30m,
        ABS(requester_id - 9) AS requester_id_distance,
        LOWER(TRIM(channel)) AS channel_clean
    FROM filtered_tickets
)
SELECT
    priority,
    agent_id,
    PERCENTILE_APPROX(csat, 0.5) AS median_csat,
    SUM(greatest_requester_id_csat) AS total_greatest_requester_id_csat,
    COUNT(*) AS row_count
FROM scored
GROUP BY ROLLUP(priority, agent_id)
ORDER BY priority NULLS LAST, agent_id NULLS LAST
