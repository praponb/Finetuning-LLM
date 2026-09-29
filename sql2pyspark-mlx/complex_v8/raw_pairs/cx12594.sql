WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        csat,
        closed_at + INTERVAL 30 MINUTES AS closed_at_plus_30m
    FROM tickets
    WHERE channel = 'phone'
      AND csat BETWEEN 2 AND 4
),
deduped AS (
    -- Keep the latest row per agent_id, channel
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        csat,
        closed_at_plus_30m
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, channel ORDER BY closed_at_plus_30m DESC, id DESC) AS recency_rank
        FROM scoped
    ) ranked
    WHERE recency_rank = 1
),
agent_id_stats AS (
    -- Avg csat per agent_id
    SELECT agent_id, AVG(csat) AS avg_csat
    FROM deduped
    GROUP BY agent_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_csat, ROUND(s.csat - g.avg_csat, 2) AS csat_minus_avg
    FROM deduped s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.csat > g.avg_csat
)
SELECT id, metric_name, amount
FROM (
    SELECT id, avg_csat, csat_minus_avg
    FROM outliers
) src
UNPIVOT (
    amount FOR metric_name IN (avg_csat, csat_minus_avg)
) unp
