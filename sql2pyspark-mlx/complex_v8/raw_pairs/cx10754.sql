WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-12 11:00:00'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        priority_slug,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER w_csat_priority_by_requester_i)) / 60, 1) AS minutes_since_prev_closed_at,
        ROUND(csat - AVG(csat) OVER (PARTITION BY TO_DATE(closed_at), agent_id), 2) AS csat_vs_closed_at_to_date_agent_id_avg,
        COUNT(*) OVER (PARTITION BY TO_DATE(closed_at), agent_id) AS rows_in_closed_at_to_date_agent_id,
        requester_id - LAG(requester_id) OVER (PARTITION BY agent_id, channel ORDER BY requester_id ASC, id) AS requester_id_change,
        LAG(closed_at) OVER (PARTITION BY csat, priority ORDER BY requester_id ASC, id) AS prev_closed_at,
        ROUND(csat - AVG(csat) OVER w_agent_id_csat, 2) AS csat_vs_agent_id_csat_avg,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY csat, priority ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        NTILE(5) OVER (PARTITION BY agent_id, channel ORDER BY requester_id ASC, id) AS requester_id_quintile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id, YEAR(closed_at) ORDER BY opened_at DESC), 3) AS opened_at_percent_rank,
        COUNT(*) OVER (PARTITION BY agent_id, csat) AS rows_in_agent_id_csat,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY csat, priority ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        ROW_NUMBER() OVER (PARTITION BY agent_id, channel ORDER BY requester_id ASC, id) AS rn,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY TO_DATE(closed_at), agent_id), 2) AS requester_id_vs_closed_at_to_date_agent_id_avg
    FROM scoped
    WINDOW w_csat_priority_by_requester_i AS (PARTITION BY csat, priority ORDER BY requester_id DESC, id), w_agent_id_csat AS (PARTITION BY agent_id, csat)
)
SELECT
    id,
    agent_id,
    closed_at,
    priority,
    priority_slug,
    csat_vs_closed_at_to_date_agent_id_avg,
    requester_id_change,
    csat_vs_agent_id_csat_avg,
    second_requester_id,
    requester_id_quintile,
    opened_at_percent_rank,
    rows_in_agent_id_csat,
    second_opened_at,
    rn,
    requester_id_vs_closed_at_to_date_agent_id_avg,
    DATE_TRUNC('MONTH', closed_at) AS closed_at_month
FROM with_windows
ORDER BY rn DESC, priority_slug
