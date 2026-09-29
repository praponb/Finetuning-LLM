WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        HOUR(opened_at) AS opened_at_hour,
        COALESCE(requester_id, 0) AS requester_id_filled,
        requester_id + csat AS requester_id_plus_csat
    FROM tickets
    WHERE (HOUR(closed_at) BETWEEN 9 AND 15 OR MONTH(opened_at) IN (2, 3))
),
unique_rows AS (
    -- Keep the earliest row per channel
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        opened_at_hour,
        requester_id_filled,
        requester_id_plus_csat
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel ORDER BY opened_at_hour, id) AS rn
        FROM base
    ) ranked
    WHERE rn = 1
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat,
        opened_at_hour,
        requester_id_filled,
        NTILE(4) OVER (PARTITION BY channel ORDER BY opened_at_hour, id) AS opened_at_hour_quartile,
        LAG(requester_id) OVER (PARTITION BY agent_id ORDER BY opened_at_hour, id) AS prev_requester_id,
        DENSE_RANK() OVER w_agent_id_by_opened_at_hour AS opened_at_hour_dense_rank,
        ROUND(requester_id_filled - AVG(requester_id_filled) OVER w_agent_id_opened_at_hour, 2) AS requester_id_filled_vs_agent_id_opened_at_hour_a,
        ROUND(opened_at_hour / NULLIF(SUM(opened_at_hour) OVER (PARTITION BY agent_id, opened_at_hour), 0), 4) AS opened_at_hour_share_of_agent_id_opened_at_hour,
        COUNT(*) OVER (PARTITION BY agent_id, opened_at_hour) AS rows_in_agent_id_opened_at_hour
    FROM unique_rows
    WINDOW w_agent_id_by_opened_at_hour AS (PARTITION BY agent_id ORDER BY opened_at_hour, id), w_agent_id_opened_at_hour AS (PARTITION BY agent_id, opened_at_hour)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(requester_id_filled - AVG(requester_id_filled) OVER (PARTITION BY priority), 2) AS requester_id_filled_vs_priority_avg,
        MIN(opened_at_hour_quartile) OVER (ORDER BY requester_id_filled NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_opened_at_hour_quartile_so_far
    FROM sequenced
),
with_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.csat,
        s.requester_id_filled,
        s.prev_requester_id,
        s.rows_in_agent_id_opened_at_hour,
        s.requester_id_filled_vs_priority_avg,
        s.min_opened_at_hour_quartile_so_far,
        su.agent_name,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM with_trends s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.hired_on IS NOT NULL
),
joined AS (
    -- Attach agent attributes
    SELECT s.*, su.tier AS agent_tier
    FROM with_details_agent s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MAX(opened_at) AS last_ticket_opened_at,
        COUNT(*) AS ticket_count,
        MIN(opened_at) AS first_ticket_opened_at
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.last_ticket_opened_at,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.first_ticket_opened_at
    FROM joined s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
-- Final projection
SELECT
    id,
    requester_id,
    closed_at,
    requester_id_filled,
    prev_requester_id,
    min_opened_at_hour_quartile_so_far,
    agent_team,
    agent_tier,
    last_ticket_opened_at,
    ticket_count,
    first_ticket_opened_at,
    prev_requester_id - ticket_count AS prev_requester_id_minus_ticket_count,
    LENGTH(TRIM(agent_team)) AS agent_team_len
FROM with_counts
WHERE min_opened_at_hour_quartile_so_far > 41
ORDER BY first_ticket_opened_at DESC NULLS FIRST
