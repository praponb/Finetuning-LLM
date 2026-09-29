WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        DATE_TRUNC('YEAR', opened_at) AS opened_at_year,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 6 AND 8
      AND agent_id >= 9
),
deduped AS (
    -- Keep the latest row per channel_or_unknown
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        opened_at_year,
        channel_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel_or_unknown ORDER BY requester_id DESC, id DESC) AS recency_rank
        FROM recent_tickets
    ) ranked
    WHERE recency_rank = 1
),
with_details AS (
    -- Join support_agents
    SELECT s.*, su.team AS agent_team, su.hired_on AS agent_hired_on
    FROM deduped s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team NOT IN ('VIP', 'Onboarding')
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', opened_at_year) AS days_since_opened_at_year
    FROM with_details
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        opened_at_year,
        channel_or_unknown,
        agent_hired_on,
        days_since_opened_at_year,
        CONCAT(channel_or_unknown, '#', CAST(requester_id AS STRING)) AS channel_or_unknown_ref
    FROM tagged
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.csat > 2
        )
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    channel,
    agent_hired_on,
    days_since_opened_at_year,
    channel_or_unknown_ref,
    SUBSTRING(TRIM(channel_or_unknown_ref), 1, 2) AS channel_or_unknown_ref_prefix,
    COALESCE(channel, 'UNKNOWN') AS channel_or_unknown_2
FROM with_activity
WHERE requester_id <= 19
ORDER BY channel_or_unknown_ref_prefix
