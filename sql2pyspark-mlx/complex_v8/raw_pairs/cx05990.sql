WITH valid AS (
    -- Keep valid rows and derive helper columns
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
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id
    FROM tickets
    WHERE channel <> 'chat'
      AND NOT ARRAY_CONTAINS(tags, 'bug')
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug,
        GREATEST(csat, requester_id) AS greatest_csat_requester_id
    FROM valid
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        tags,
        csat_per_requester_id,
        greatest_csat_requester_id,
        MIN(csat_per_requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), TO_DATE(closed_at)) AS opened_at_month_closed_at_to_date_min_csat_per_r,
        SIZE(COLLECT_SET(priority) OVER w_opened_at_month_closed_at_to) AS distinct_priority_in_opened_at_month_closed_at_t,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), TO_DATE(closed_at))) AS distinct_channel_in_opened_at_month_closed_at_to,
        LAST_VALUE(greatest_csat_requester_id) IGNORE NULLS OVER (PARTITION BY channel ORDER BY csat_per_requester_id NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_greatest_csat_requester_id,
        RANK() OVER (PARTITION BY channel ORDER BY csat_per_requester_id NULLS LAST) AS csat_per_requester_id_rank
    FROM derived
    WINDOW w_opened_at_month_closed_at_to AS (PARTITION BY DATE_TRUNC('MONTH', opened_at), TO_DATE(closed_at))
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY agent_id) AS rows_in_agent_id,
        DENSE_RANK() OVER (PARTITION BY distinct_channel_in_opened_at_month_closed_at_to ORDER BY csat_per_requester_id_rank ASC) AS csat_per_requester_id_rank_dense_rank,
        LEAD(requester_id) OVER (PARTITION BY agent_id ORDER BY closed_at ASC NULLS LAST, id) AS next_requester_id,
        MIN(greatest_csat_requester_id) OVER (PARTITION BY agent_id) AS agent_id_min_greatest_csat_requester_id,
        csat_per_requester_id - LAG(csat_per_requester_id) OVER w_agent_id_by_closed_at AS csat_per_requester_id_change,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY agent_id), 2) AS requester_id_vs_agent_id_avg
    FROM sequenced
    WINDOW w_agent_id_by_closed_at AS (PARTITION BY agent_id ORDER BY closed_at ASC NULLS LAST, id)
)
-- Subtotals with GROUPING SETS
SELECT
    csat_per_requester_id_rank_dense_rank,
    distinct_priority_in_opened_at_month_closed_at_t,
    distinct_channel_in_opened_at_month_closed_at_to,
    GROUPING(csat_per_requester_id_rank_dense_rank) AS is_total_csat_per_requester_id_rank_dense_rank,
    MIN(closed_at) AS first_closed_at,
    MAX(closed_at) AS last_closed_at,
    ROUND(AVG(distinct_channel_in_opened_at_month_closed_at_to), 2) AS avg_distinct_channel_in_opened_at_month_closed_a
FROM windowed
GROUP BY GROUPING SETS ((csat_per_requester_id_rank_dense_rank, distinct_priority_in_opened_at_month_closed_at_t, distinct_channel_in_opened_at_month_closed_at_to), (csat_per_requester_id_rank_dense_rank), ())
ORDER BY csat_per_requester_id_rank_dense_rank NULLS LAST, distinct_priority_in_opened_at_month_closed_at_t NULLS LAST, distinct_channel_in_opened_at_month_closed_at_to NULLS LAST
