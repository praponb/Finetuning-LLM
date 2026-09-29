WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        CONCAT(channel, '#', CAST(csat AS STRING)) AS channel_ref
    FROM tickets
    WHERE channel = 'email'
      AND csat > 3
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        csat - LAG(csat) OVER w_all_by_requester_id AS csat_change,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY csat ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        NTH_VALUE(csat, 2) OVER (ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        ROW_NUMBER() OVER (ORDER BY requester_id DESC, id) AS seq_no,
        ROUND(AVG(requester_id) OVER (ORDER BY csat, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_2_prior,
        COUNT(*) OVER w_closed_at_month_agent_id AS rows_in_closed_at_month_agent_id,
        LAG(csat) OVER (ORDER BY csat, id) AS prev_csat,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER w_closed_at_month_agent_id_by)) / 60, 1) AS minutes_since_prev_closed_at,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY agent_id, csat), 0), 4) AS csat_share_of_agent_id_csat,
        NTILE(4) OVER w_all_by_csat AS csat_quartile,
        LEAD(closed_at) OVER (ORDER BY csat, id) AS next_closed_at,
        ROUND(csat - AVG(csat) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at), agent_id), 2) AS csat_vs_closed_at_month_agent_id_avg,
        ROUND(csat - AVG(csat) OVER (PARTITION BY TO_DATE(closed_at)), 2) AS csat_vs_closed_at_to_date_avg,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at), agent_id ORDER BY closed_at NULLS LAST, id) AS closed_at_rank
    FROM scoped
    WINDOW w_all_by_requester_id AS (ORDER BY requester_id DESC, id), w_closed_at_month_agent_id AS (PARTITION BY DATE_TRUNC('MONTH', closed_at), agent_id), w_closed_at_month_agent_id_by AS (PARTITION BY DATE_TRUNC('MONTH', closed_at), agent_id ORDER BY closed_at NULLS LAST, id), w_all_by_csat AS (ORDER BY csat, id)
)
-- Final projection
SELECT
    id,
    requester_id,
    closed_at,
    csat,
    csat_change,
    second_requester_id,
    seq_no,
    requester_id_avg_2_prior,
    rows_in_closed_at_month_agent_id,
    minutes_since_prev_closed_at,
    csat_share_of_agent_id_csat,
    csat_vs_closed_at_month_agent_id_avg,
    csat_vs_closed_at_to_date_avg,
    closed_at_rank,
    CASE WHEN prev_csat >= 4 THEN 'HIGH' WHEN prev_csat >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS prev_csat_tier,
    requester_id_avg_2_prior + csat_quartile AS requester_id_avg_2_prior_plus_csat_quartile
FROM metrics
ORDER BY second_requester_id, seq_no DESC
