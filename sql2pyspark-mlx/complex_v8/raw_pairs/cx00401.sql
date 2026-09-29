WITH valid_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        CONCAT_WS('|', tier, team) AS tier_team_label,
        DATE_FORMAT(hired_on, 'MMM yyyy') AS hired_on_label
    FROM support_agents
    WHERE tier = 'T1'
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        agent_name,
        tier,
        tier_team_label,
        COALESCE(tier, 'UNKNOWN') AS tier_or_unknown
    FROM valid_agents
),
qualified AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.id
              AND t.priority = 'P2'
        )
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.id) AS first_ticket_closed_at,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.id) AS first_ticket_closed_at_2
    FROM qualified s
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_name,
        tier_team_label,
        tier_or_unknown,
        first_ticket_closed_at_2,
        LAG(first_ticket_closed_at_2) OVER w_tier_or_unknown_by_first_tic AS prev_first_ticket_closed_at_2,
        ROUND((UNIX_TIMESTAMP(first_ticket_closed_at_2) - UNIX_TIMESTAMP(LAG(first_ticket_closed_at_2) OVER (PARTITION BY tier_or_unknown ORDER BY first_ticket_closed_at DESC, id))) / 60, 1) AS minutes_since_prev_first_ticket_closed_at_2,
        RANK() OVER (PARTITION BY tier_or_unknown ORDER BY first_ticket_closed_at_2 ASC, id) AS first_ticket_closed_at_2_rank,
        ROUND((UNIX_TIMESTAMP(first_ticket_closed_at_2) - UNIX_TIMESTAMP(LAG(first_ticket_closed_at_2) OVER (PARTITION BY tier, tier_or_unknown ORDER BY first_ticket_closed_at_2 DESC, id))) / 60, 1) AS minutes_since_prev_first_ticket_closed_at_2_2,
        LAG(first_ticket_closed_at) OVER (PARTITION BY tier_or_unknown ORDER BY first_ticket_closed_at_2 ASC, id) AS prev_first_ticket_closed_at
    FROM profiled
    WINDOW w_tier_or_unknown_by_first_tic AS (PARTITION BY tier_or_unknown ORDER BY first_ticket_closed_at DESC, id)
),
filtered AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.id
              AND MONTH(t.closed_at) IN (1, 2)
              AND t.requester_id > 11
        )
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(first_ticket_closed_at_2, 2) OVER (PARTITION BY YEAR(first_ticket_closed_at_2) ORDER BY prev_first_ticket_closed_at_2 NULLS LAST, id) AS next2_first_ticket_closed_at_2,
        RANK() OVER (PARTITION BY MONTH(prev_first_ticket_closed_at) ORDER BY prev_first_ticket_closed_at ASC) AS prev_first_ticket_closed_at_rank
    FROM filtered
)
SELECT
    id,
    agent_name,
    tier_or_unknown,
    first_ticket_closed_at_2,
    minutes_since_prev_first_ticket_closed_at_2_2,
    next2_first_ticket_closed_at_2,
    prev_first_ticket_closed_at_rank
FROM with_trends
WHERE LENGTH(TRIM(agent_name)) >= 8
ORDER BY agent_name, tier_or_unknown DESC
