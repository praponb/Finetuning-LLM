WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        ADD_MONTHS(hired_on, 1) AS hired_on_plus_1m,
        QUARTER(hired_on) AS hired_on_quarter,
        LOWER(TRIM(tier)) AS tier_clean
    FROM support_agents
    WHERE team IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        tier,
        hired_on_quarter,
        DATEDIFF(hired_on_plus_1m, LAG(hired_on_plus_1m) OVER (PARTITION BY hired_on_quarter, team ORDER BY hired_on_quarter, id)) AS days_since_prev_hired_on_plus_1m,
        RANK() OVER (PARTITION BY team ORDER BY hired_on_plus_1m, id) AS hired_on_plus_1m_rank,
        LAG(hired_on_plus_1m, 3) OVER (PARTITION BY hired_on_quarter, team ORDER BY hired_on_quarter, id) AS prev3_hired_on_plus_1m,
        MAX(hired_on_quarter) OVER (PARTITION BY hired_on_quarter, team ORDER BY hired_on_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_hired_on_quarter_so_far,
        FIRST_VALUE(hired_on_plus_1m) OVER (PARTITION BY hired_on_quarter, team ORDER BY hired_on_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_hired_on_plus_1m,
        ROUND(AVG(hired_on_quarter) OVER (PARTITION BY hired_on_quarter, team ORDER BY hired_on_quarter, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS hired_on_quarter_avg_3
    FROM valid
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MAX(csat) AS max_ticket_csat,
        SUM(csat) AS total_ticket_csat,
        MAX(closed_at) AS last_ticket_closed_at
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.max_ticket_csat,
        t.total_ticket_csat,
        t.last_ticket_closed_at
    FROM metrics s
    LEFT JOIN ticket_stats t ON t.agent_id = s.id
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM with_stats
    WHERE hired_on_plus_1m_rank <= 5
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(max_ticket_csat, hired_on_quarter_avg_3) AS greatest_max_ticket_csat_hired_on_quarter_avg_3,
        GREATEST(hired_on_plus_1m_rank, hired_on_quarter) AS greatest_hired_on_plus_1m_rank_hired_on_quarter,
        ROUND(hired_on_quarter_avg_3 * 1.1, 2) AS hired_on_quarter_avg_3_adj
    FROM first_rows
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        tier,
        hired_on_quarter,
        days_since_prev_hired_on_plus_1m,
        first_hired_on_plus_1m,
        hired_on_quarter_avg_3,
        total_ticket_csat,
        last_ticket_closed_at,
        greatest_max_ticket_csat_hired_on_quarter_avg_3,
        hired_on_quarter_avg_3_adj,
        ROUND(hired_on_quarter_avg_3 * 0.9, 2) AS hired_on_quarter_avg_3_adj_2
    FROM tagged
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        tier,
        days_since_prev_hired_on_plus_1m,
        first_hired_on_plus_1m,
        last_ticket_closed_at,
        hired_on_quarter_avg_3_adj_2,
        hired_on_quarter_avg_3 - total_ticket_csat AS hired_on_quarter_avg_3_minus_total_ticket_csat
    FROM scored
    WHERE MONTH(last_ticket_closed_at) IN (2, 3)
)
SELECT
    id,
    days_since_prev_hired_on_plus_1m,
    first_hired_on_plus_1m,
    last_ticket_closed_at,
    hired_on_quarter_avg_3_adj_2,
    UPPER(TRIM(tier)) AS tier_clean,
    DATE_FORMAT(first_hired_on_plus_1m, 'MMM yyyy') AS first_hired_on_plus_1m_label
FROM derived
ORDER BY tier_clean DESC
