WITH base_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown,
        DATEDIFF(DATE '2025-12-31', hired_on) AS days_since_hired_on
    FROM support_agents
    WHERE team = 'Tech'
      AND hired_on < DATE '2025-02-27'
),
filtered AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM base_agents s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.id
              AND NOT ARRAY_CONTAINS(t.tags, 'upgrade')
              AND HOUR(t.closed_at) BETWEEN 8 AND 13
        )
      AND s.team <> 'Onboarding'
),
latest_per_key AS (
    -- Keep the latest row per team, team_or_unknown
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        team_or_unknown,
        days_since_hired_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team, team_or_unknown ORDER BY days_since_hired_on DESC, id DESC) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        team,
        team_or_unknown,
        SUM(days_since_hired_on) FILTER (WHERE team <> 'Billing') AS days_since_hired_on_team_billing,
        MAX(days_since_hired_on) AS max_days_since_hired_on
    FROM latest_per_key
    WHERE team_or_unknown IN ('Tech', 'Billing')
    GROUP BY team, team_or_unknown
)
-- Native SQL UNPIVOT (wide to long)
SELECT team, team_or_unknown, metric_name, amount
FROM (
    SELECT team, team_or_unknown, max_days_since_hired_on, days_since_hired_on_team_billing
    FROM stats
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric_name IN (max_days_since_hired_on AS m_max_days_since_hired_on, days_since_hired_on_team_billing AS days_since_hired_on_team_billing_v)
) unp
