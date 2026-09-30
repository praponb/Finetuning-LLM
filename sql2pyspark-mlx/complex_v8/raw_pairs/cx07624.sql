WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        csat,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE SIZE(tags) >= 2
      AND HOUR(closed_at) BETWEEN 7 AND 9
),
with_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.channel,
        s.channel_len,
        su.team AS agent_team,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM valid s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_closed_at,
        (SELECT MIN(t.opened_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_opened_at
    FROM with_details s
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(channel_len / 2) * 2 AS channel_len_bucket,
        ABS(requester_id - 13) AS requester_id_distance,
        FLOOR(channel_len / 2) * 2 AS channel_len_bucket_2
    FROM profiled
    WHERE MONTH(first_ticket_opened_at) IN (2, 3)
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(agent_team, 'ALL') AS agent_team,
    channel_len_bucket,
    GROUPING_ID() AS gid,
    SUM(CASE WHEN agent_team = 'VIP' THEN channel_len ELSE 0 END) AS channel_len_agent_team_vip
FROM enriched
GROUP BY GROUPING SETS ((agent_team, channel_len_bucket), (agent_team), ())
