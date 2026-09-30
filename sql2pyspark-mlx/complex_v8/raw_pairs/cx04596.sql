WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        tags,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat,
        CASE WHEN requester_id >= 21 THEN 'A' WHEN requester_id >= 8 THEN 'B' ELSE 'C' END AS requester_id_tier
    FROM tickets
    WHERE channel = 'email'
      AND TO_DATE(opened_at) < DATE '2025-02-06'
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(requester_id - 7) AS requester_id_distance,
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at,
        MONTH(closed_at) AS closed_at_month
    FROM cleaned
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_id,
        CASE WHEN requester_id_per_csat >= 5.5 THEN 'large' WHEN requester_id_per_csat >= 3.0 THEN 'medium' ELSE 'small' END AS requester_id_per_csat_tier,
        MAX(closed_at_month) AS max_closed_at_month,
        SUM(CASE WHEN channel = 'email' THEN 1 ELSE 0 END) AS n_channel_email
    FROM scored
    GROUP BY 1, 2
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT agent_id, requester_id_per_csat_tier, max_closed_at_month
    FROM stats
) src
PIVOT (
    SUM(max_closed_at_month)
    FOR requester_id_per_csat_tier IN ('small', 'medium')
)
ORDER BY agent_id
