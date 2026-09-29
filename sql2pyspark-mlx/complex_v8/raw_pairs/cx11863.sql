WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        CASE WHEN requester_id >= 17 THEN 'HIGH' WHEN requester_id >= 8 THEN 'MEDIUM' ELSE 'LOW' END AS requester_id_tier,
        COALESCE(requester_id, 0) AS requester_id_filled
    FROM tickets
    WHERE agent_id <= 7
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, su.agent_name, su.tier AS agent_tier
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        requester_id_tier,
        agent_tier,
        CASE WHEN requester_id_filled >= 20 THEN 'large' WHEN requester_id_filled >= 11 THEN 'medium' ELSE 'small' END AS requester_id_filled_tier
    FROM enriched
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), csat) AS opened_at_month_csat_max_requester_id,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER w_opened_at_month_csat, 0), 4) AS requester_id_share_of_opened_at_month_csat,
        RANK() OVER (ORDER BY requester_id DESC, id) AS requester_id_rank,
        DENSE_RANK() OVER w_agent_tier_csat_by_requester AS requester_id_dense_rank
    FROM annotated
    WINDOW w_opened_at_month_csat AS (PARTITION BY DATE_TRUNC('MONTH', opened_at), csat), w_agent_tier_csat_by_requester AS (PARTITION BY agent_tier, csat ORDER BY requester_id)
),
screened AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_type IN ('assigned', 'resolved', 'escalated')
        )
),
grouped AS (
    -- Group-level totals
    SELECT
        requester_id_tier,
        CASE WHEN requester_id >= 18 THEN 'A' WHEN requester_id >= 7 THEN 'B' ELSE 'C' END AS requester_id_tier_2,
        YEAR(opened_at) AS opened_at_year,
        COUNT(DISTINCT requester_id_tier) AS distinct_requester_id_tiers,
        ROUND(STDDEV(csat), 2) AS stddev_csat,
        PERCENTILE_APPROX(csat, 0.5) AS median_csat,
        COUNT(*) AS cnt,
        SUM(requester_id) AS total_requester_id,
        COUNT(*) FILTER (WHERE agent_tier IS NOT NULL) AS n_agent_tier_is_not_null
    FROM screened
    WHERE agent_id IS NOT NULL
    GROUP BY requester_id_tier, CASE WHEN requester_id >= 18 THEN 'A' WHEN requester_id >= 7 THEN 'B' ELSE 'C' END, YEAR(opened_at)
    HAVING COUNT(*) >= 2
)
SELECT
    requester_id_tier,
    requester_id_tier_2,
    opened_at_year,
    distinct_requester_id_tiers,
    stddev_csat,
    median_csat,
    cnt,
    total_requester_id,
    ROUND(opened_at_year / NULLIF(n_agent_tier_is_not_null, 0), 3) AS opened_at_year_per_n_agent_tier_is_not_null
FROM grouped
WHERE requester_id_tier_2 = 'A'
ORDER BY requester_id_tier DESC
