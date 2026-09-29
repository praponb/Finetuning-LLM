WITH prepared_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags,
        ARRAY_CONTAINS(tags, 'refund') AS has_refund,
        ABS(requester_id - 13) AS requester_id_distance
    FROM tickets
    WHERE priority <> 'P2'
      AND channel = 'chat'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        COUNT(*) OVER (PARTITION BY csat, has_refund) AS rows_in_csat_has_refund,
        ROUND(requester_id_distance - AVG(requester_id_distance) OVER (PARTITION BY csat, has_refund), 2) AS requester_id_distance_vs_csat_has_refund_avg,
        DENSE_RANK() OVER w_opened_at_month_agent_id_by AS csat_dense_rank,
        NTILE(10) OVER (PARTITION BY has_refund ORDER BY closed_at ASC, id) AS closed_at_decile,
        NTILE(3) OVER (PARTITION BY has_refund ORDER BY closed_at ASC, id) AS closed_at_tercile,
        SUM(requester_id_distance) OVER w_csat_has_refund AS csat_has_refund_sum_requester_id_distance,
        ROUND(PERCENT_RANK() OVER (PARTITION BY has_refund ORDER BY closed_at ASC), 3) AS closed_at_percent_rank,
        ROUND(PERCENT_RANK() OVER w_has_refund_by_closed_at, 3) AS closed_at_percent_rank_2
    FROM prepared_tickets
    WINDOW w_opened_at_month_agent_id_by AS (PARTITION BY MONTH(opened_at), agent_id ORDER BY csat DESC), w_csat_has_refund AS (PARTITION BY csat, has_refund), w_has_refund_by_closed_at AS (PARTITION BY has_refund ORDER BY closed_at ASC, id)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM sequenced
    WHERE csat_dense_rank <= 5
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        rows_in_csat_has_refund,
        requester_id_distance_vs_csat_has_refund_avg,
        csat_dense_rank,
        closed_at_decile,
        closed_at_tercile,
        csat_has_refund_sum_requester_id_distance,
        closed_at_percent_rank,
        closed_at_percent_rank_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat_dense_rank, agent_id ORDER BY requester_id_distance_vs_csat_has_refund_avg NULLS LAST, id), 3) AS requester_id_distance_vs_csat_has_refund_avg_per,
        SIZE(COLLECT_SET(agent_id) OVER w_opened_at_to_date_closed_at) AS distinct_agent_id_in_opened_at_to_date_closed_at,
        ROUND(csat_dense_rank - AVG(csat_dense_rank) OVER w_opened_at_to_date_closed_at, 2) AS csat_dense_rank_vs_opened_at_to_date_closed_at_d,
        COUNT(*) OVER (PARTITION BY TO_DATE(opened_at), closed_at_decile) AS rows_in_opened_at_to_date_closed_at_decile,
        ROUND(closed_at_percent_rank_2 - AVG(closed_at_percent_rank_2) OVER (PARTITION BY TO_DATE(opened_at), closed_at_decile), 2) AS closed_at_percent_rank_2_vs_opened_at_to_date_cl,
        NTH_VALUE(closed_at_percent_rank, 2) OVER (PARTITION BY closed_at_decile ORDER BY csat_has_refund_sum_requester_id_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at_percent_rank
    FROM top_rows
    WINDOW w_opened_at_to_date_closed_at AS (PARTITION BY TO_DATE(opened_at), closed_at_decile)
)
SELECT
    id,
    rows_in_csat_has_refund,
    csat_dense_rank,
    closed_at_decile,
    closed_at_percent_rank,
    closed_at_percent_rank_2,
    requester_id_distance_vs_csat_has_refund_avg_per,
    distinct_agent_id_in_opened_at_to_date_closed_at,
    rows_in_opened_at_to_date_closed_at_decile,
    closed_at_percent_rank_2_vs_opened_at_to_date_cl,
    LEAST(closed_at_decile, closed_at_tercile) AS least_closed_at_decile_closed_at_tercile
FROM windowed
ORDER BY csat_dense_rank DESC, least_closed_at_decile_closed_at_tercile DESC
