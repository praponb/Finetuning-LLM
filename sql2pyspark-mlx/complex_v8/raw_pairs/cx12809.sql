WITH scoped_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT_WS('-', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND csat <= 5
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.channel_priority_label,
        su.agent_name
    FROM scoped_tickets s
    JOIN support_agents su ON su.id = s.agent_id AND LENGTH(TRIM(su.agent_name)) >= 8
    WHERE LENGTH(TRIM(su.agent_name)) >= 7
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        channel_priority_label,
        UPPER(TRIM(agent_name)) AS agent_name_clean
    FROM with_parent_agent
),
with_activity AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM annotated s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.csat BETWEEN 2 AND 5
        )
),
exploded AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.csat,
        s.channel_priority_label,
        s.agent_name_clean,
        t.tag
    FROM with_activity s
    LATERAL VIEW OUTER EXPLODE(s.tags) t AS tag
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        csat,
        agent_name_clean,
        tag,
        FIRST_VALUE(csat) OVER (PARTITION BY priority ORDER BY requester_id NULLS LAST, id, tag ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        MIN(csat) OVER (PARTITION BY agent_id, tag) AS agent_id_tag_min_csat,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_id, tag), 0), 4) AS requester_id_share_of_agent_id_tag,
        ROUND(PERCENT_RANK() OVER w_agent_id_csat_by_requester_i, 3) AS requester_id_percent_rank,
        DENSE_RANK() OVER (PARTITION BY csat ORDER BY csat) AS csat_dense_rank
    FROM exploded
    WINDOW w_agent_id_csat_by_requester_i AS (PARTITION BY agent_id, csat ORDER BY requester_id DESC)
)
SELECT
    id,
    requester_id,
    csat,
    agent_name_clean,
    tag,
    agent_id_tag_min_csat,
    requester_id_percent_rank,
    csat_dense_rank,
    FLOOR(requester_id / 5) * 5 AS requester_id_bucket,
    COALESCE(first_csat, 0) AS first_csat_filled
FROM sequenced
ORDER BY first_csat_filled DESC
