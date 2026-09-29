WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat,
        DATE_TRUNC('YEAR', closed_at) AS closed_at_year
    FROM tickets
    WHERE (channel IS NOT NULL OR csat > 1)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(csat) OVER (PARTITION BY agent_id ORDER BY requester_id DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_1_prior,
        NTILE(10) OVER (PARTITION BY agent_id ORDER BY requester_id DESC, id) AS requester_id_decile,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id ORDER BY requester_id DESC, id), 3) AS requester_id_cume_dist,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id ORDER BY requester_id DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_2_prior
    FROM cleaned_tickets
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM windowed
    WHERE requester_id_cume_dist > (SELECT AVG(requester_id_cume_dist) FROM windowed)
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_closed_at
    FROM qualified s
)
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    priority,
    csat,
    closed_at_year,
    csat_avg_1_prior,
    requester_id_decile,
    requester_id_cume_dist,
    requester_id_avg_2_prior,
    last_ticket_closed_at,
    CONCAT(priority, '#', CAST(requester_id AS STRING)) AS priority_ref,
    LENGTH(TRIM(priority)) AS priority_len
FROM summarized_rows
ORDER BY closed_at DESC NULLS FIRST, closed_at_year, id
LIMIT 20
