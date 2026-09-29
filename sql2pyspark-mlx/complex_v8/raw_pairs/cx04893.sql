WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 9 AND 12
),
agent_id_stats AS (
    -- Median csat per agent_id
    SELECT agent_id, PERCENTILE_APPROX(csat, 0.5) AS median_csat
    FROM cleaned_tickets
    GROUP BY agent_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_csat, ROUND(s.csat - g.median_csat, 2) AS csat_minus_median
    FROM cleaned_tickets s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.csat >= g.median_csat
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) AS ticket_count,
        COUNT(*) AS ticket_count_2,
        MIN(csat) AS min_ticket_csat
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        COALESCE(t.ticket_count_2, 0) AS ticket_count_2,
        t.min_ticket_csat
    FROM compared s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        CASE WHEN requester_id >= 18 THEN 'A' WHEN requester_id >= 9 THEN 'B' ELSE 'C' END AS requester_id_tier,
        priority,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT channel) AS distinct_channels
    FROM summarized_rows
    GROUP BY 1, 2
    HAVING SUM(csat_minus_median) > -1.5
)
SELECT requester_id_tier, priority, field, value
FROM (
    SELECT requester_id_tier, priority, n_rows, distinct_channels
    FROM rollup_base
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (n_rows AS n_rows_v, distinct_channels AS distinct_channels_v)
) unp
ORDER BY requester_id_tier, priority, field
