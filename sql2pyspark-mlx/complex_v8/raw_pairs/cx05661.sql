WITH recent AS (
    -- Scope the tickets rows
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
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at
    FROM tickets
    WHERE agent_id IS NOT NULL
),
grouped AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', closed_at) AS closed_at_month,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        SUM(CASE WHEN priority IN ('P1', 'P2') THEN 1 ELSE 0 END) AS n_priority_in_p1_p2,
        SUM(csat) AS total_csat,
        ROUND(AVG(csat), 2) AS avg_csat,
        SUM(days_since_closed_at) AS total_days_since_closed_at,
        MAX(days_since_closed_at) AS max_days_since_closed_at,
        ROUND(SUM(days_since_closed_at) / NULLIF(SUM(requester_id), 0), 3) AS days_since_closed_at_to_requester_id_ratio
    FROM recent
    GROUP BY DATE_TRUNC('MONTH', closed_at), DATE_TRUNC('MONTH', opened_at)
    HAVING AVG(requester_id) > 7
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM grouped
    WHERE avg_csat > (SELECT MIN(avg_csat) FROM grouped)
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN total_csat >= 21 THEN 'large' WHEN total_csat >= 11 THEN 'medium' ELSE 'small' END AS total_csat_tier,
        COALESCE(total_csat, 0) AS total_csat_filled,
        DATEDIFF(DATE '2025-06-30', closed_at_month) AS days_since_closed_at_month
    FROM qualified
),
tagged AS (
    -- Label and score rows
    SELECT
        closed_at_month,
        opened_at_month,
        total_csat,
        avg_csat,
        total_days_since_closed_at,
        max_days_since_closed_at,
        days_since_closed_at_to_requester_id_ratio,
        total_csat_tier,
        total_csat_filled,
        days_since_closed_at_month,
        ABS(max_days_since_closed_at - 210) AS max_days_since_closed_at_distance,
        CASE WHEN total_csat >= 17 THEN 'HIGH' WHEN total_csat >= 14 THEN 'MEDIUM' ELSE 'LOW' END AS total_csat_tier_2
    FROM enriched
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM tagged
    WHERE total_days_since_closed_at > (SELECT MIN(total_days_since_closed_at) FROM tagged)
),
scored AS (
    -- Add calculated columns
    SELECT
        closed_at_month,
        opened_at_month,
        avg_csat,
        total_days_since_closed_at,
        max_days_since_closed_at,
        days_since_closed_at_to_requester_id_ratio,
        total_csat_tier,
        total_csat_filled,
        days_since_closed_at_month,
        max_days_since_closed_at_distance,
        total_csat_tier_2,
        COALESCE(avg_csat, 0.0) AS avg_csat_filled,
        COALESCE(total_csat_tier, 'UNKNOWN') AS total_csat_tier_or_unknown
    FROM with_activity
)
SELECT
    closed_at_month,
    avg_csat,
    total_days_since_closed_at,
    max_days_since_closed_at,
    days_since_closed_at_to_requester_id_ratio,
    total_csat_tier,
    total_csat_filled,
    days_since_closed_at_month,
    max_days_since_closed_at_distance,
    total_csat_tier_2,
    avg_csat_filled
FROM scored
WHERE total_csat_tier_or_unknown IN ('medium', 'UNKNOWN')
ORDER BY total_csat_tier DESC
