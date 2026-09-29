WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.csat) AS prior_csat_1d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(prior_rows_1d - AVG(prior_rows_1d) OVER (PARTITION BY agent_id), 2) AS prior_rows_1d_vs_agent_id_avg,
        prior_rows_1d - LAG(prior_rows_1d) OVER w_opened_at_to_date_prior_rows AS prior_rows_1d_change,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(opened_at), prior_rows_1d ORDER BY opened_at) AS opened_at_dense_rank,
        ROUND(AVG(prior_csat_1d) OVER (PARTITION BY TO_DATE(opened_at), prior_rows_1d ORDER BY opened_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS prior_csat_1d_avg_4,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY TO_DATE(opened_at), prior_rows_1d ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at
    FROM matched
    WINDOW w_opened_at_to_date_prior_rows AS (PARTITION BY TO_DATE(opened_at), prior_rows_1d ORDER BY opened_at, id)
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, ROUND(AVG(requester_id), 2) AS avg_ticket_requester_id
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.avg_ticket_requester_id
    FROM with_windows s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        agent_id,
        prior_rows_1d,
        prior_rows_1d_vs_agent_id_avg,
        prior_rows_1d_change,
        opened_at_dense_rank,
        last_known_opened_at,
        avg_ticket_requester_id,
        CASE WHEN prior_csat_1d_avg_4 >= 21.2 THEN 'HIGH' WHEN prior_csat_1d_avg_4 >= 8.8 THEN 'MEDIUM' ELSE 'LOW' END AS prior_csat_1d_avg_4_tier
    FROM with_counts
    WHERE opened_at_dense_rank <= 6
)
SELECT id, metric_name, amount
FROM (
    SELECT id, prior_rows_1d_vs_agent_id_avg, avg_ticket_requester_id
    FROM annotated
) src
UNPIVOT (
    amount FOR metric_name IN (prior_rows_1d_vs_agent_id_avg AS m_prior_rows_1d_vs_agent_id_avg, avg_ticket_requester_id AS avg_ticket_requester_id_v)
) unp
ORDER BY id, metric_name
