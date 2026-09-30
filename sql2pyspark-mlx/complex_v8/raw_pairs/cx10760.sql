WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        COALESCE(requester_id, 0) AS requester_id_filled,
        LOWER(TRIM(priority)) AS priority_clean,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM tickets
    WHERE priority IN ('P2', 'P4')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        priority_clean,
        MAX(requester_id_filled) OVER (PARTITION BY priority, channel ORDER BY requester_id_filled DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS requester_id_filled_max_3_prior,
        ROW_NUMBER() OVER (PARTITION BY priority, channel ORDER BY requester_id_filled DESC, id) AS requester_id_filled_seq,
        ROUND(requester_id_filled / NULLIF(SUM(requester_id_filled) OVER w_priority, 0), 4) AS requester_id_filled_share_of_priority,
        MIN(requester_id) OVER (PARTITION BY priority) AS priority_min_requester_id,
        RANK() OVER w_priority_channel_by_requeste_3 AS requester_id_filled_rank
    FROM cleaned
    WINDOW w_priority AS (PARTITION BY priority), w_priority_channel_by_requeste_3 AS (PARTITION BY priority, channel ORDER BY requester_id_filled DESC)
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        priority_clean,
        requester_id_filled_max_3_prior,
        requester_id_filled_seq,
        requester_id_filled_share_of_priority,
        priority_min_requester_id,
        LENGTH(TRIM(priority)) AS priority_len,
        LOWER(REGEXP_REPLACE(TRIM(priority_clean), ' ', '_')) AS priority_clean_slug,
        GREATEST(requester_id_filled_rank, requester_id) AS greatest_requester_id_filled_rank_requester_id
    FROM with_windows
),
per_group AS (
    -- Summarise per group
    SELECT
        channel,
        agent_id,
        ROUND(AVG(requester_id_filled_seq), 2) AS avg_requester_id_filled_seq,
        ROUND(100 * COUNT(CASE WHEN priority <> 'P1' THEN 1 END) / COUNT(*), 1) AS pct_priority_p1,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        MAX(priority_min_requester_id) AS max_priority_min_requester_id,
        MIN(requester_id) AS min_requester_id
    FROM labeled
    GROUP BY 1, 2
)
SELECT channel, agent_id, field, amount
FROM (
    SELECT channel, agent_id, pct_priority_p1, avg_requester_id_filled_seq
    FROM per_group
) src
UNPIVOT INCLUDE NULLS (
    amount FOR field IN (pct_priority_p1, avg_requester_id_filled_seq)
) unp
ORDER BY channel, agent_id, field
