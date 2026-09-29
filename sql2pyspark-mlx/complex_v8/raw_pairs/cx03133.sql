WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        tags,
        DATE_TRUNC('HOUR', opened_at) AS opened_at_hour
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-12 10:00:00'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(opened_at_hour, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY requester_id ASC NULLS LAST, id) AS prev3_opened_at_hour,
        RANK() OVER w_all_by_closed_at_ties AS closed_at_rank,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY csat, MONTH(opened_at_hour)), 0), 4) AS requester_id_share_of_csat_opened_at_hour_month,
        MAX(csat) OVER (ORDER BY closed_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS csat_max_2,
        MAX(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY requester_id ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS requester_id_max_3,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY agent_id), 0), 4) AS csat_share_of_agent_id,
        FIRST_VALUE(csat) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY requester_id ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        ROW_NUMBER() OVER (ORDER BY requester_id NULLS LAST, id) AS seq_no,
        ROUND(csat - AVG(csat) OVER (PARTITION BY agent_id), 2) AS csat_vs_agent_id_avg,
        ROUND(AVG(csat) OVER (ORDER BY closed_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_2_prior,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY csat, agent_id ORDER BY opened_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER w_agent_id, 0), 4) AS requester_id_share_of_agent_id,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY requester_id ASC NULLS LAST) AS requester_id_rank
    FROM cleaned
    WINDOW w_all_by_closed_at_ties AS (ORDER BY closed_at), w_agent_id AS (PARTITION BY agent_id)
),
leaders AS (
    -- Top 5 per group
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        tags,
        opened_at_hour,
        prev3_opened_at_hour,
        requester_id_share_of_csat_opened_at_hour_month,
        csat_max_2,
        requester_id_max_3,
        csat_share_of_agent_id,
        first_csat,
        seq_no,
        csat_vs_agent_id_avg,
        csat_avg_2_prior,
        first_known_requester_id,
        requester_id_share_of_agent_id,
        requester_id_rank
    FROM windowed
    WHERE closed_at_rank <= 5
      AND csat_vs_agent_id_avg BETWEEN -2.5 AND 2.5
)
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    csat,
    tags,
    opened_at_hour,
    prev3_opened_at_hour,
    first_csat,
    seq_no,
    csat_vs_agent_id_avg,
    csat_avg_2_prior,
    requester_id_rank,
    COALESCE(csat_vs_agent_id_avg, 0.0) AS csat_vs_agent_id_avg_filled
FROM leaders
WHERE agent_id IS NOT NULL
ORDER BY csat NULLS LAST, opened_at_hour DESC, id
LIMIT 5
