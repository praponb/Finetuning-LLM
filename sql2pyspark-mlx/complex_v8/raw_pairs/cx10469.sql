WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-02-02 03:00:00'
      AND channel IS NOT NULL
),
combined AS (
    -- Stack the branches
    SELECT csat AS ref_id, channel_ref AS category
    FROM scoped_tickets
    UNION ALL
    SELECT requester_id, priority
    FROM tickets
)
SELECT category, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id
FROM combined
GROUP BY category
ORDER BY category
