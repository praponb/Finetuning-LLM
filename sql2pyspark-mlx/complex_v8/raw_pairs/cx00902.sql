WITH prepared_tickets AS (
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
        tags
    FROM tickets
    WHERE requester_id BETWEEN 5 AND 23
      AND SIZE(tags) >= 1
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY priority)) AS distinct_channel_in_priority,
        NTH_VALUE(opened_at, 2) OVER (ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY priority)) AS distinct_agent_id_in_priority,
        RANK() OVER w_all_by_requester_id_ties AS requester_id_rank
    FROM prepared_tickets
    WINDOW w_all_by_requester_id_ties AS (ORDER BY requester_id DESC)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(requester_id_rank / 2) * 2 AS requester_id_rank_bucket,
        ROUND(csat / NULLIF(requester_id_rank, 0), 3) AS csat_per_requester_id_rank,
        CASE WHEN requester_id >= 19 THEN 'A' WHEN requester_id >= 8 THEN 'B' ELSE 'C' END AS requester_id_tier
    FROM windowed
)
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    tags,
    second_opened_at,
    requester_id_rank,
    csat_per_requester_id_rank,
    requester_id_tier,
    CASE WHEN csat >= 4 THEN 'A' WHEN csat >= 3 THEN 'B' ELSE 'C' END AS csat_tier,
    ROUND(requester_id_rank_bucket / NULLIF(requester_id, 0), 3) AS requester_id_rank_bucket_per_requester_id
FROM annotated
ORDER BY requester_id_rank DESC, csat_per_requester_id_rank
