WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        tags,
        GREATEST(requester_id, csat) AS greatest_requester_id_csat
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 6 AND 12
      AND requester_id < 23
),
per_label AS (
    -- One row per tag
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.greatest_requester_id_csat,
        t.tag_pos,
        t.tag
    FROM recent s
    LATERAL VIEW OUTER POSEXPLODE(s.tags) t AS tag_pos, tag
),
deduped AS (
    -- Keep the earliest row per tag
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        greatest_requester_id_csat,
        tag_pos,
        tag
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tag ORDER BY tag_pos, id, tag_pos) AS recency_rank
        FROM per_label
    ) ranked
    WHERE recency_rank = 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        tag_pos,
        tag,
        NTILE(5) OVER (PARTITION BY tag ORDER BY opened_at NULLS LAST, id, tag_pos) AS opened_at_quintile,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY channel)) AS distinct_agent_id_in_channel,
        NTILE(5) OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id, tag_pos) AS opened_at_quintile_2,
        LAST_VALUE(greatest_requester_id_csat) OVER (PARTITION BY channel ORDER BY greatest_requester_id_csat, id, tag_pos ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_greatest_requester_id_csat
    FROM deduped
)
SELECT
    id,
    agent_id,
    opened_at,
    tag_pos,
    tag,
    opened_at_quintile_2
FROM ranked
ORDER BY agent_id DESC NULLS FIRST
