WITH scoped_agents AS (
    -- Filter support_agents
    SELECT *
    FROM support_agents
    WHERE tier IN ('T2', 'T3')
      AND LOWER(agent_name) LIKE '%iz%'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_name,
        tier,
        hired_on,
        ROUND(PERCENT_RANK() OVER (ORDER BY hired_on, id), 3) AS hired_on_percent_rank,
        ROW_NUMBER() OVER w_all_by_hired_on AS hired_on_seq,
        DENSE_RANK() OVER (ORDER BY hired_on) AS hired_on_dense_rank,
        ROUND(CUME_DIST() OVER (ORDER BY hired_on, id), 3) AS hired_on_cume_dist,
        DENSE_RANK() OVER (ORDER BY hired_on, id) AS hired_on_dense_rank_2,
        ROUND(PERCENT_RANK() OVER w_all_by_hired_on_ties, 3) AS hired_on_percent_rank_2
    FROM scoped_agents
    WINDOW w_all_by_hired_on AS (ORDER BY hired_on, id), w_all_by_hired_on_ties AS (ORDER BY hired_on)
),
tier_stats AS (
    -- Median hired_on_dense_rank per tier
    SELECT tier, PERCENTILE_APPROX(hired_on_dense_rank, 0.5) AS median_hired_on_dense_rank
    FROM windowed
    GROUP BY tier
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_hired_on_dense_rank,
        ROUND(s.hired_on_dense_rank - g.median_hired_on_dense_rank, 2) AS hired_on_dense_rank_minus_median
    FROM windowed s
    JOIN tier_stats g ON g.tier = s.tier
    WHERE s.hired_on_dense_rank >= g.median_hired_on_dense_rank
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) AS ticket_count,
        MAX(opened_at) AS last_ticket_opened_at,
        MIN(opened_at) AS first_ticket_opened_at
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.last_ticket_opened_at,
        t.first_ticket_opened_at
    FROM above_average s
    LEFT JOIN ticket_stats t ON t.agent_id = s.id
),
deduped AS (
    -- Keep the latest row per tier
    SELECT
        id,
        agent_name,
        tier,
        hired_on,
        hired_on_percent_rank,
        hired_on_seq,
        hired_on_dense_rank,
        hired_on_cume_dist,
        hired_on_dense_rank_2,
        hired_on_percent_rank_2,
        median_hired_on_dense_rank,
        hired_on_dense_rank_minus_median,
        ticket_count,
        last_ticket_opened_at,
        first_ticket_opened_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tier ORDER BY hired_on_percent_rank_2 DESC, id DESC) AS rn
        FROM summarized_rows
    ) ranked
    WHERE rn = 1
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        agent_name,
        tier,
        hired_on,
        hired_on_dense_rank,
        hired_on_cume_dist,
        hired_on_dense_rank_2,
        median_hired_on_dense_rank,
        hired_on_dense_rank_minus_median,
        ticket_count,
        last_ticket_opened_at,
        first_ticket_opened_at,
        DATEDIFF(DATE '2025-06-30', first_ticket_opened_at) AS days_since_first_ticket_opened_at,
        SUBSTRING(TRIM(agent_name), 1, 2) AS agent_name_prefix,
        ROUND((UNIX_TIMESTAMP(last_ticket_opened_at) - UNIX_TIMESTAMP(first_ticket_opened_at)) / 60, 1) AS minutes_first_ticket_opened_at_to_last_ticket_op
    FROM deduped
    WHERE ticket_count > 2
)
SELECT
    id,
    agent_name,
    hired_on,
    hired_on_dense_rank,
    hired_on_cume_dist,
    hired_on_dense_rank_2,
    median_hired_on_dense_rank,
    ticket_count,
    last_ticket_opened_at,
    first_ticket_opened_at,
    days_since_first_ticket_opened_at,
    minutes_first_ticket_opened_at_to_last_ticket_op
FROM scored
WHERE hired_on_dense_rank_2 < 7
ORDER BY days_since_first_ticket_opened_at, hired_on_cume_dist
