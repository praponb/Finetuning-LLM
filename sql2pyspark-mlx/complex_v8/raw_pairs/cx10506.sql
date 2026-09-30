WITH recent AS (
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
        requester_id + csat AS requester_id_plus_csat,
        csat + requester_id AS csat_plus_requester_id
    FROM tickets
    WHERE priority NOT IN ('P4', 'P3')
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(requester_id, csat_plus_requester_id) AS least_requester_id_csat_plus_requester_id,
        LENGTH(TRIM(priority)) AS priority_len
    FROM recent
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY channel ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER w_csat_channel_by_requester_id AS requester_id_dense_rank
    FROM annotated
    WINDOW w_csat_channel_by_requester_id AS (PARTITION BY csat, channel ORDER BY requester_id DESC, id)
),
annotated_2 AS (
    -- Add calculated columns
    SELECT *, priority_len + requester_id AS priority_len_plus_requester_id
    FROM ranked
    WHERE least_requester_id_csat_plus_requester_id BETWEEN 10 AND 21
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        csat_plus_requester_id,
        least_requester_id_csat_plus_requester_id,
        priority_len,
        LEAST(priority_len, csat_plus_requester_id) AS least_priority_len_csat_plus_requester_id
    FROM annotated_2
    WHERE running_count <= 14
)
SELECT *
FROM (
    SELECT agent_id, priority_len, priority, least_priority_len_csat_plus_requester_id
    FROM scored
) src
PIVOT (
    MIN(least_priority_len_csat_plus_requester_id)
    FOR priority IN ('P4', 'P2')
)
ORDER BY agent_id, priority_len
