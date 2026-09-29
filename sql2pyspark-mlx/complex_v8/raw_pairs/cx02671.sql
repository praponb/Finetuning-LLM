WITH base_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        csat - requester_id AS csat_minus_requester_id
    FROM tickets
    WHERE priority NOT IN ('P4', 'P3')
      AND channel = 'web'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(opened_at), csat ORDER BY closed_at, id), 3) AS closed_at_percent_rank,
        MAX(csat) OVER (PARTITION BY channel ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_csat_so_far,
        MIN(csat) OVER (PARTITION BY priority ORDER BY csat_minus_requester_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_csat_so_far,
        ROW_NUMBER() OVER (PARTITION BY csat, channel ORDER BY closed_at, id) AS closed_at_seq,
        NTILE(4) OVER (PARTITION BY channel ORDER BY closed_at, id) AS closed_at_quartile,
        COUNT(*) OVER (PARTITION BY YEAR(opened_at)) AS rows_in_opened_at_year,
        SUM(csat_minus_requester_id) OVER (PARTITION BY YEAR(opened_at)) AS opened_at_year_sum_csat_minus_requester_id,
        RANK() OVER w_csat_channel_by_closed_at AS closed_at_rank,
        csat - LAG(csat) OVER (PARTITION BY csat ORDER BY csat, id) AS csat_change
    FROM base_tickets
    WINDOW w_csat_channel_by_closed_at AS (PARTITION BY csat, channel ORDER BY closed_at, id)
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat_minus_requester_id,
        closed_at_percent_rank,
        closed_at_seq,
        closed_at_quartile,
        rows_in_opened_at_year,
        closed_at_rank,
        csat_change,
        ROUND(closed_at_rank - AVG(closed_at_rank) OVER (PARTITION BY closed_at_seq, closed_at_rank), 2) AS closed_at_rank_vs_closed_at_seq_closed_at_rank_a,
        COUNT(*) OVER (PARTITION BY closed_at_seq, closed_at_rank) AS rows_in_closed_at_seq_closed_at_rank
    FROM ranked
    WINDOW w_closed_at_seq_closed_at_rank AS (PARTITION BY closed_at_seq, closed_at_rank)
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    channel,
    csat_minus_requester_id,
    closed_at_percent_rank,
    closed_at_seq,
    closed_at_quartile,
    rows_in_opened_at_year,
    closed_at_rank,
    csat_change,
    rows_in_closed_at_seq_closed_at_rank,
    COALESCE(closed_at_quartile, 0) AS closed_at_quartile_filled,
    ROUND(closed_at_percent_rank * 0.9, 2) AS closed_at_percent_rank_adj
FROM ranked_2
ORDER BY opened_at DESC NULLS FIRST, id NULLS LAST
