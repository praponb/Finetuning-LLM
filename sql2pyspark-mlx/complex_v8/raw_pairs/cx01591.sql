WITH prepared_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        csat,
        tags,
        CONCAT(channel, '#', CAST(agent_id AS STRING)) AS channel_ref,
        csat + requester_id AS csat_plus_requester_id
    FROM tickets
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        csat,
        tags,
        channel_ref,
        csat_plus_requester_id,
        NTILE(3) OVER (PARTITION BY agent_id, channel ORDER BY opened_at ASC NULLS LAST, id) AS opened_at_tercile,
        SUM(csat) OVER w_csat AS csat_sum_csat,
        ROUND(AVG(csat) OVER (PARTITION BY csat), 2) AS csat_avg_csat,
        ROW_NUMBER() OVER (PARTITION BY agent_id, channel ORDER BY opened_at ASC NULLS LAST, id) AS opened_at_seq,
        NTH_VALUE(csat, 2) OVER (PARTITION BY agent_id, channel ORDER BY opened_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        ROUND(csat_plus_requester_id - AVG(csat_plus_requester_id) OVER (PARTITION BY csat), 2) AS csat_plus_requester_id_vs_csat_avg
    FROM prepared_tickets
    WINDOW w_csat AS (PARTITION BY csat)
)
-- Subtotals with GROUPING SETS
SELECT
    opened_at_tercile,
    opened_at_seq,
    GROUPING(opened_at_tercile) AS is_total_opened_at_tercile,
    ROUND(SUM(csat_avg_csat), 2) AS total_csat_avg_csat
FROM sequenced
GROUP BY GROUPING SETS ((opened_at_tercile, opened_at_seq), (opened_at_tercile), ())
ORDER BY opened_at_tercile NULLS LAST, opened_at_seq NULLS LAST
