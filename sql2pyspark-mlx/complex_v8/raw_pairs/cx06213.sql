WITH base AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-12 08:00:00'
      AND HOUR(opened_at) BETWEEN 8 AND 12
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.csat,
        s.tags,
        s.priority_or_unknown,
        su.hired_on AS agent_hired_on
    FROM base s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.team = 'Onboarding'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(csat) OVER (PARTITION BY MONTH(closed_at)) AS closed_at_month_max_csat,
        csat - LAG(csat) OVER (ORDER BY opened_at, id) AS csat_change,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat ORDER BY agent_hired_on DESC, id), 3) AS agent_hired_on_percent_rank,
        LEAD(agent_hired_on) OVER (ORDER BY agent_hired_on DESC, id) AS next_agent_hired_on,
        ROW_NUMBER() OVER (ORDER BY opened_at, id) AS opened_at_seq,
        ROUND(AVG(csat) OVER (PARTITION BY MONTH(closed_at)), 2) AS closed_at_month_avg_csat,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY TO_DATE(closed_at)), 0), 4) AS csat_share_of_closed_at_to_date,
        SUM(csat) OVER (PARTITION BY TO_DATE(closed_at)) AS closed_at_to_date_sum_csat
    FROM combined_details_agent
),
first_rows AS (
    -- Top 2 per group
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags,
        priority_or_unknown,
        agent_hired_on,
        closed_at_month_max_csat,
        csat_change,
        agent_hired_on_percent_rank,
        next_agent_hired_on,
        closed_at_month_avg_csat,
        csat_share_of_closed_at_to_date,
        closed_at_to_date_sum_csat
    FROM windowed
    WHERE opened_at_seq <= 2
      AND closed_at_month_max_csat >= 23
)
SELECT
    id,
    agent_id,
    opened_at,
    closed_at,
    csat,
    tags,
    priority_or_unknown,
    agent_hired_on,
    closed_at_month_max_csat,
    csat_change,
    next_agent_hired_on,
    closed_at_month_avg_csat,
    csat_share_of_closed_at_to_date,
    closed_at_to_date_sum_csat,
    INITCAP(TRIM(priority_or_unknown)) AS priority_or_unknown_clean
FROM first_rows
ORDER BY csat, id
LIMIT 10
