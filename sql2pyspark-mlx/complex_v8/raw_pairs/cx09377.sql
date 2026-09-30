WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        tags,
        DATEDIFF(DATE '2026-01-01', opened_at) AS days_since_opened_at,
        DAYOFWEEK(closed_at) AS closed_at_dayofweek,
        LENGTH(TRIM(priority)) AS priority_len
    FROM tickets
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_closed_at
    FROM recent_tickets s
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        tags,
        closed_at_dayofweek,
        priority_len,
        first_ticket_closed_at,
        MIN(priority_len) OVER w_first_ticket_closed_at_year AS first_ticket_closed_at_year_min_priority_len,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY priority_len ORDER BY priority_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at,
        DENSE_RANK() OVER w_priority_len_by_priority_len_2 AS priority_len_dense_rank
    FROM profiled
    WINDOW w_first_ticket_closed_at_year AS (PARTITION BY YEAR(first_ticket_closed_at)), w_priority_len_by_priority_len_2 AS (PARTITION BY priority_len ORDER BY priority_len ASC, id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(priority_len_dense_rank) IGNORE NULLS OVER (PARTITION BY channel ORDER BY first_ticket_closed_at_year_min_priority_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_priority_len_dense_rank,
        LEAD(priority_len) OVER w_closed_at_dayofweek_priority AS next_priority_len,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY first_ticket_closed_at_year_min_priority_len DESC) AS first_ticket_closed_at_year_min_priority_len_den
    FROM with_trends
    WINDOW w_closed_at_dayofweek_priority AS (PARTITION BY closed_at_dayofweek, priority_len ORDER BY priority_len_dense_rank, id)
)
SELECT
    channel,
    first_known_priority_len_dense_rank,
    GROUPING(channel) AS is_total_channel,
    SUM(CASE WHEN channel = 'email' THEN first_ticket_closed_at_year_min_priority_len_den ELSE 0 END) AS first_ticket_closed_at_year_min_priority_len_den_2,
    COUNT(*) AS row_count,
    COUNT(DISTINCT channel) AS distinct_channels
FROM ranked
GROUP BY ROLLUP(channel, first_known_priority_len_dense_rank)
