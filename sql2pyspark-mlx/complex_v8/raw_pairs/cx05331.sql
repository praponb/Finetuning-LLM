WITH valid_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        CASE WHEN requester_id >= 18 THEN 'HIGH' WHEN requester_id >= 7 THEN 'MEDIUM' ELSE 'LOW' END AS requester_id_tier,
        TO_DATE(closed_at) AS closed_at_date
    FROM tickets
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        closed_at,
        channel,
        csat,
        requester_id_tier,
        closed_at_date,
        SUM(csat) OVER (PARTITION BY MONTH(closed_at), csat ORDER BY csat, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS csat_sum_4,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat, YEAR(closed_at_date) ORDER BY closed_at_date DESC), 3) AS closed_at_date_percent_rank,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY agent_id, DATE_TRUNC('MONTH', closed_at) ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY agent_id, requester_id_tier), 2) AS requester_id_vs_agent_id_requester_id_tier_avg,
        SUM(csat) OVER w_agent_id_requester_id_tier AS agent_id_requester_id_tier_sum_csat,
        MAX(requester_id) OVER (PARTITION BY YEAR(closed_at), requester_id_tier) AS closed_at_year_requester_id_tier_max_requester_i,
        COUNT(*) OVER (PARTITION BY channel ORDER BY closed_at_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY YEAR(closed_at), requester_id_tier), 0), 4) AS requester_id_share_of_closed_at_year_requester_i,
        ROUND(AVG(csat) OVER (PARTITION BY YEAR(closed_at), requester_id_tier), 2) AS closed_at_year_requester_id_tier_avg_csat,
        SUM(requester_id) OVER (PARTITION BY agent_id, DATE_TRUNC('MONTH', closed_at) ORDER BY closed_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS requester_id_sum_2_prior,
        COUNT(*) OVER (PARTITION BY agent_id, requester_id_tier) AS rows_in_agent_id_requester_id_tier,
        LAST_VALUE(closed_at_date) IGNORE NULLS OVER (PARTITION BY agent_id, DATE_TRUNC('MONTH', closed_at) ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at_date,
        SUM(requester_id) OVER (PARTITION BY agent_id, requester_id_tier) AS agent_id_requester_id_tier_sum_requester_id
    FROM valid_tickets
    WINDOW w_agent_id_requester_id_tier AS (PARTITION BY agent_id, requester_id_tier)
)
-- Final projection
SELECT
    id,
    requester_id,
    closed_at,
    channel,
    csat,
    csat_sum_4,
    agent_id_requester_id_tier_sum_csat,
    requester_id_share_of_closed_at_year_requester_i,
    closed_at_year_requester_id_tier_avg_csat,
    requester_id_sum_2_prior,
    rows_in_agent_id_requester_id_tier,
    last_known_closed_at_date,
    second_requester_id - requester_id_sum_2_prior AS second_requester_id_minus_requester_id_sum_2_pri
FROM windowed
ORDER BY requester_id, id
LIMIT 20
