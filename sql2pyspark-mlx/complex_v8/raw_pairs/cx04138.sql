WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        QUARTER(opened_at) AS opened_at_quarter,
        DATEDIFF(DATE '2026-01-01', closed_at) AS days_since_closed_at,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at
    FROM tickets
    WHERE TO_DATE(opened_at) < DATE '2025-02-24'
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_id,
        channel,
        MIN(closed_at) AS first_closed_at,
        MAX(minutes_opened_at_to_closed_at) AS max_minutes_opened_at_to_closed_at,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        ROUND(SUM(days_since_closed_at) / NULLIF(SUM(opened_at_quarter), 0), 3) AS days_since_closed_at_to_opened_at_quarter_ratio
    FROM cleaned_tickets
    WHERE MONTH(opened_at) IN (2, 3)
    GROUP BY 1, 2
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.agent_id,
        s.channel,
        s.first_closed_at,
        s.max_minutes_opened_at_to_closed_at,
        s.days_since_closed_at_to_opened_at_quarter_ratio,
        su.tier AS agent_tier
    FROM summary s
    JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        agent_id,
        channel,
        first_closed_at,
        days_since_closed_at_to_opened_at_quarter_ratio,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY days_since_closed_at_to_opened_at_quarter_ratio DESC, agent_id, channel) AS rn
    FROM enriched
),
without_matches AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM with_trends s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.priority <> 'P1'
        )
      AND s.agent_id <= 9
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(rn) OVER (PARTITION BY rn, channel), 2) AS rn_channel_avg_rn,
        MIN(rn) OVER (PARTITION BY rn, channel) AS rn_channel_min_rn,
        RANK() OVER (PARTITION BY agent_id ORDER BY days_since_closed_at_to_opened_at_quarter_ratio ASC, agent_id, channel) AS days_since_closed_at_to_opened_at_quarter_ratio_,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY rn, channel)) AS distinct_agent_id_in_rn_channel
    FROM without_matches
),
screened AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND ARRAY_CONTAINS(t.tags, 'bug')
        )
      AND s.rn <= 9
)
SELECT DISTINCT rn_channel_avg_rn, first_closed_at
FROM screened
ORDER BY first_closed_at DESC, rn_channel_avg_rn DESC NULLS FIRST
