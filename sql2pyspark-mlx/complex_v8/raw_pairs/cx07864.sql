WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at_2
    FROM ticket_events
    WHERE event_type = 'escalated'
      AND event_at >= TIMESTAMP '2025-02-06 09:00:00'
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.days_since_event_at_2,
        t.priority AS ticket_priority,
        t.agent_id AS ticket_agent_id,
        t.channel AS ticket_channel,
        su.tier AS agent_tier,
        su.agent_name
    FROM valid s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        agent_tier,
        PERCENTILE_APPROX(days_since_event_at_2, 0.5) AS median_days_since_event_at_2,
        MAX(days_since_event_at_2) AS max_days_since_event_at_2,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list
    FROM enriched_ticket
    WHERE LOWER(agent_name) LIKE '%ir%'
    GROUP BY agent_tier
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE cnt > (SELECT AVG(cnt) FROM rollup_base)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(agent_tier, '#', CAST(cnt AS STRING)) AS agent_tier_ref,
        LOWER(REGEXP_REPLACE(TRIM(agent_tier), ' ', '_')) AS agent_tier_slug
    FROM qualified
    WHERE cnt <= 8
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(agent_tier, 'UNKNOWN') AS agent_tier_or_unknown,
        FLOOR(cnt / 2) * 2 AS cnt_bucket
    FROM tagged
),
agent_tier_or_unknown_stats AS (
    -- Avg median_days_since_event_at_2 per agent_tier_or_unknown
    SELECT agent_tier_or_unknown, AVG(median_days_since_event_at_2) AS avg_median_days_since_event_at_2
    FROM derived
    GROUP BY agent_tier_or_unknown
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_median_days_since_event_at_2,
        ROUND(s.median_days_since_event_at_2 - g.avg_median_days_since_event_at_2, 2) AS median_days_since_event_at_2_minus_avg
    FROM derived s
    JOIN agent_tier_or_unknown_stats g ON g.agent_tier_or_unknown = s.agent_tier_or_unknown
    WHERE s.median_days_since_event_at_2 > g.avg_median_days_since_event_at_2
)
-- Final projection
SELECT
    agent_tier,
    median_days_since_event_at_2,
    max_days_since_event_at_2,
    agent_name_list,
    agent_tier_slug,
    agent_tier_or_unknown,
    median_days_since_event_at_2_minus_avg
FROM above_average
ORDER BY agent_tier_slug DESC, median_days_since_event_at_2 NULLS LAST
