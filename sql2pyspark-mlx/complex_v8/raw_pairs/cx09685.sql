WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        COALESCE(requester_id, 0) AS requester_id_filled,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket,
        LOWER(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE (ARRAY_CONTAINS(tags, 'refund') OR requester_id <= 18)
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT s.*, su.team AS agent_team
    FROM recent s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND LOWER(su.agent_name) LIKE '%na%'
),
agent_team_stats AS (
    -- Avg requester_id_bucket per agent_team
    SELECT agent_team, AVG(requester_id_bucket) AS avg_requester_id_bucket
    FROM with_parent_agent
    GROUP BY agent_team
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id_bucket,
        ROUND(s.requester_id_bucket - g.avg_requester_id_bucket, 2) AS requester_id_bucket_minus_avg
    FROM with_parent_agent s
    JOIN agent_team_stats g ON g.agent_team = s.agent_team
    WHERE s.requester_id_bucket > g.avg_requester_id_bucket
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        requester_id_bucket,
        priority_clean,
        agent_team,
        avg_requester_id_bucket,
        requester_id_bucket_minus_avg,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at,
        opened_at + INTERVAL 1 DAYS AS opened_at_plus_1d
    FROM compared
),
deduped AS (
    -- Keep the earliest row per agent_team
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        requester_id_bucket,
        priority_clean,
        agent_team,
        avg_requester_id_bucket,
        requester_id_bucket_minus_avg,
        minutes_opened_at_to_closed_at,
        opened_at_plus_1d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_team ORDER BY minutes_opened_at_to_closed_at, id) AS row_num
        FROM derived
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    agent_id,
    opened_at,
    closed_at,
    tags,
    requester_id_bucket,
    priority_clean,
    agent_team,
    avg_requester_id_bucket,
    requester_id_bucket_minus_avg,
    ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text,
    LENGTH(TRIM(priority_clean)) AS priority_clean_len
FROM deduped
ORDER BY opened_at NULLS LAST
