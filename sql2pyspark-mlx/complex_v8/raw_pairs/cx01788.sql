WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        COALESCE(requester_id, 0) AS requester_id_filled,
        requester_id + csat AS requester_id_plus_csat
    FROM tickets
    WHERE channel <> 'email'
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        tags,
        requester_id_filled,
        requester_id_plus_csat,
        COALESCE(csat, 0) AS csat_filled,
        ROUND(csat / NULLIF(requester_id_plus_csat, 0), 3) AS csat_per_requester_id_plus_csat
    FROM filtered_tickets
    WHERE csat < 4
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        requester_id_filled - requester_id_plus_csat AS requester_id_filled_minus_requester_id_plus_csat,
        CASE WHEN priority IN ('P2', 'P3') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM derived
    WHERE priority = 'P1'
),
scored AS (
    -- Add calculated columns
    SELECT *, closed_at + INTERVAL 1 DAYS AS closed_at_plus_1d
    FROM enriched
)
SELECT id, field, amount
FROM (
    SELECT id, csat_filled, requester_id_plus_csat
    FROM scored
) src
UNPIVOT (
    amount FOR field IN (csat_filled, requester_id_plus_csat)
) unp
