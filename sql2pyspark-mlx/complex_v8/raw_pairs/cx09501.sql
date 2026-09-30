WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        csat,
        GREATEST(csat, requester_id) AS greatest_csat_requester_id
    FROM tickets
    WHERE requester_id >= 7
      AND csat >= 2
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(4) OVER (ORDER BY greatest_csat_requester_id DESC, id) AS greatest_csat_requester_id_quartile,
        DENSE_RANK() OVER w_all_by_greatest_csat_request_2 AS greatest_csat_requester_id_dense_rank,
        COUNT(*) OVER (PARTITION BY csat, agent_id) AS rows_in_csat_agent_id,
        LAG(csat, 3) OVER w_all_by_greatest_csat_request AS prev3_csat,
        RANK() OVER (ORDER BY greatest_csat_requester_id DESC) AS greatest_csat_requester_id_rank
    FROM recent_tickets
    WINDOW w_all_by_greatest_csat_request_2 AS (ORDER BY greatest_csat_requester_id DESC), w_all_by_greatest_csat_request AS (ORDER BY greatest_csat_requester_id DESC, id)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        csat,
        greatest_csat_requester_id,
        greatest_csat_requester_id_quartile,
        rows_in_csat_agent_id,
        prev3_csat,
        greatest_csat_requester_id_rank
    FROM sequenced
    WHERE greatest_csat_requester_id_dense_rank <= 5
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    csat,
    greatest_csat_requester_id_quartile,
    prev3_csat,
    greatest_csat_requester_id_rank,
    LEAST(requester_id, prev3_csat) AS least_requester_id_prev3_csat
FROM top_rows
ORDER BY agent_id, least_requester_id_prev3_csat
