WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CASE WHEN priority IN ('P1', 'P2') THEN 'group_a' WHEN priority = 'P3' THEN 'group_b' ELSE 'other' END AS priority_group,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM tickets
    WHERE csat >= 3
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(csat - AVG(csat) OVER (PARTITION BY csat, channel), 2) AS csat_vs_csat_channel_avg,
        ROUND(requester_id_bucket - AVG(requester_id_bucket) OVER (PARTITION BY csat, channel), 2) AS requester_id_bucket_vs_csat_channel_avg,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY priority, agent_id ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at
    FROM scoped_tickets
),
totals AS (
    -- Group-level totals
    SELECT
        YEAR(opened_at) AS opened_at_year,
        DATE_TRUNC('MONTH', closed_at) AS closed_at_month,
        ROUND(SUM(requester_id_bucket) / NULLIF(SUM(requester_id_bucket_vs_csat_channel_avg), 0), 3) AS requester_id_bucket_to_requester_id_bucket_vs_cs,
        ROUND(SUM(requester_id_bucket_vs_csat_channel_avg) / NULLIF(SUM(csat_vs_csat_channel_avg), 0), 3) AS requester_id_bucket_vs_csat_channel_avg_to_csat_,
        COUNT(*) AS n_rows
    FROM ranked
    GROUP BY YEAR(opened_at), DATE_TRUNC('MONTH', closed_at)
)
-- Native SQL UNPIVOT (wide to long)
SELECT opened_at_year, closed_at_month, field, metric_value
FROM (
    SELECT opened_at_year, closed_at_month, requester_id_bucket_vs_csat_channel_avg_to_csat_, requester_id_bucket_to_requester_id_bucket_vs_cs
    FROM totals
) src
UNPIVOT (
    metric_value FOR field IN (requester_id_bucket_vs_csat_channel_avg_to_csat_ AS requester_id_bucket_vs_csat_channel_avg_to_csat__2, requester_id_bucket_to_requester_id_bucket_vs_cs AS m_requester_id_bucket_to_requester_id_bucket_vs_)
) unp
