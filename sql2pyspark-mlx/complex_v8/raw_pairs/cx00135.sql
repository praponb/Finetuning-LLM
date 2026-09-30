WITH unioned AS (
    -- Stack the branches
    SELECT
        'ticket' AS src,
        id,
        opened_at,
        channel
    FROM tickets
    WHERE MONTH(closed_at) IN (1, 3)
      AND ARRAY_CONTAINS(tags, 'login')
    UNION ALL
    SELECT
        'ticket_2' AS src,
        agent_id,
        opened_at,
        channel
    FROM tickets
)
SELECT src, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(opened_at) AS latest_opened_at
FROM unioned
GROUP BY src
ORDER BY src
