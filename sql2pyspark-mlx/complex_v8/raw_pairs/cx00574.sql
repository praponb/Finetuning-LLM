WITH filtered_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM support_agents
    WHERE UPPER(TRIM(agent_name)) LIKE 'K%'
      AND hired_on < DATE '2024-12-31'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (ORDER BY hired_on DESC NULLS FIRST, id) AS hired_on_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY hired_on DESC NULLS FIRST), 3) AS hired_on_percent_rank
    FROM filtered_agents
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM metrics
    WHERE hired_on_rank > (SELECT AVG(hired_on_rank) FROM metrics)
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM filtered s
    LEFT JOIN ticket_stats t ON t.agent_id = s.id
),
without_matches AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM summarized_rows s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.id
              AND ARRAY_CONTAINS(t.tags, 'bug')
        )
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN tier IN ('T3', 'T1') THEN 'group_a' WHEN tier = 'T2' THEN 'group_b' ELSE 'other' END AS tier_group,
        CASE WHEN hired_on_rank >= 6 THEN 'HIGH' WHEN hired_on_rank >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS hired_on_rank_tier,
        ROUND(hired_on_percent_rank / NULLIF(ticket_count, 0), 3) AS hired_on_percent_rank_per_ticket_count
    FROM without_matches
)
SELECT
    id,
    team,
    tier,
    hired_on,
    hired_on_rank,
    hired_on_percent_rank,
    ticket_count,
    tier_group,
    hired_on_percent_rank_per_ticket_count
FROM enriched
ORDER BY hired_on_percent_rank
