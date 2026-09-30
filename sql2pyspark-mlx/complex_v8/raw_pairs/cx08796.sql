WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        CASE WHEN csat >= 4 THEN 'A' WHEN csat >= 2 THEN 'B' ELSE 'C' END AS csat_tier
    FROM tickets
    WHERE csat < 4
),
joined AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.csat,
        su.agent_name
    FROM valid_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        agent_name,
        ROUND(csat - AVG(csat) OVER (PARTITION BY YEAR(closed_at)), 2) AS csat_vs_closed_at_year_avg,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY YEAR(closed_at))) AS distinct_agent_id_in_closed_at_year,
        MAX(csat) OVER (PARTITION BY agent_id, csat ORDER BY csat DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS csat_max_1_prior,
        ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY opened_at ASC NULLS LAST, id) AS rn,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY opened_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at,
        csat - LAG(csat) OVER (ORDER BY closed_at, id) AS csat_change,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(opened_at), agent_id ORDER BY opened_at), 3) AS opened_at_cume_dist,
        COUNT(*) OVER (PARTITION BY csat, agent_id) AS rows_in_csat_agent_id,
        FIRST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY opened_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat,
        MIN(csat) OVER (PARTITION BY YEAR(closed_at)) AS closed_at_year_min_csat
    FROM joined
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        closed_at,
        csat,
        agent_name,
        csat_vs_closed_at_year_avg,
        csat_max_1_prior,
        rn,
        last_known_closed_at,
        opened_at_cume_dist,
        rows_in_csat_agent_id,
        first_known_csat,
        NTH_VALUE(last_known_closed_at, 2) OVER (PARTITION BY rn, csat ORDER BY rn DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_known_closed_at,
        MAX(rn) OVER (PARTITION BY distinct_agent_id_in_closed_at_year ORDER BY last_known_closed_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rn_max_3_prior,
        MAX(opened_at_cume_dist) OVER (PARTITION BY first_known_csat, DATE_TRUNC('MONTH', closed_at)) AS first_known_csat_closed_at_month_max_opened_at_c,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY first_known_csat, DATE_TRUNC('MONTH', closed_at))) AS distinct_agent_id_in_first_known_csat_closed_at_
    FROM with_windows
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    agent_name,
    csat_vs_closed_at_year_avg,
    csat_max_1_prior,
    rn,
    opened_at_cume_dist,
    rows_in_csat_agent_id,
    first_known_csat,
    rn_max_3_prior,
    first_known_csat_closed_at_month_max_opened_at_c,
    distinct_agent_id_in_first_known_csat_closed_at_,
    rows_in_csat_agent_id - rn_max_3_prior AS rows_in_csat_agent_id_minus_rn_max_3_prior,
    closed_at + INTERVAL 15 MINUTES AS closed_at_plus_15m
FROM metrics
ORDER BY rn, id DESC
