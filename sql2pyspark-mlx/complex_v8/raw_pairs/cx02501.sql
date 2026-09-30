WITH cleaned_agents AS (
    -- Filter support_agents
    SELECT
        id,
        team,
        tier,
        hired_on,
        COALESCE(tier, 'UNKNOWN') AS tier_or_unknown
    FROM support_agents
    WHERE tier <> 'T2'
      AND team <> 'Onboarding'
),
with_activity AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM cleaned_agents s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.id
              AND t.priority = 'P2'
              AND t.opened_at < TIMESTAMP '2025-02-12 14:00:00'
        )
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.id) AS ticket_count,
        (SELECT MIN(t.opened_at) FROM tickets t WHERE t.agent_id = s.id) AS first_ticket_opened_at,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.id) AS last_ticket_closed_at
    FROM with_activity s
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM profiled
    WHERE ticket_count > (SELECT MIN(ticket_count) FROM profiled)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        team,
        tier,
        hired_on,
        tier_or_unknown,
        ticket_count,
        SUBSTRING(TRIM(tier_or_unknown), 1, 2) AS tier_or_unknown_prefix
    FROM without_matches
)
SELECT
    id,
    tier_or_unknown,
    ticket_count,
    tier_or_unknown_prefix,
    DATEDIFF(DATE '2025-06-30', hired_on) AS days_since_hired_on,
    COALESCE(ticket_count, 0) AS ticket_count_filled
FROM derived
WHERE tier_or_unknown_prefix IS NOT NULL
ORDER BY days_since_hired_on, tier_or_unknown_prefix, id
LIMIT 10
