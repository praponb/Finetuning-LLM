WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        CONCAT(channel, '#', CAST(id AS STRING)) AS channel_ref,
        DATEDIFF(DATE '2025-12-31', closed_at) AS days_since_closed_at
    FROM tickets
    WHERE TO_DATE(closed_at) < DATE '2025-02-14'
),
with_details AS (
    -- Attach agent attributes
    SELECT s.*, su.hired_on AS agent_hired_on
    FROM valid s
    JOIN support_agents su ON su.id = s.agent_id AND su.agent_name IS NOT NULL
    WHERE su.hired_on >= DATE '2024-05-02'
),
agent_id_stats AS (
    -- Max days_since_closed_at per agent_id
    SELECT agent_id, MAX(days_since_closed_at) AS max_days_since_closed_at
    FROM with_details
    GROUP BY agent_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_days_since_closed_at,
        ROUND(s.days_since_closed_at - g.max_days_since_closed_at, 2) AS days_since_closed_at_minus_max
    FROM with_details s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.days_since_closed_at = g.max_days_since_closed_at
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY agent_id ORDER BY max_days_since_closed_at ASC) AS max_days_since_closed_at_rank,
        COUNT(*) OVER (PARTITION BY agent_id, YEAR(agent_hired_on)) AS rows_in_agent_id_agent_hired_on_year
    FROM vs_group
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_ym,
        max_days_since_closed_at_rank,
        ROUND(SUM(rows_in_agent_id_agent_hired_on_year) / NULLIF(SUM(days_since_closed_at), 0), 3) AS rows_in_agent_id_agent_hired_on_year_to_days_sin,
        MAX(closed_at) AS last_closed_at,
        COUNT(*) AS cnt
    FROM ranked
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
scored AS (
    -- Label and score rows
    SELECT
        opened_at_ym,
        max_days_since_closed_at_rank,
        rows_in_agent_id_agent_hired_on_year_to_days_sin,
        last_closed_at,
        cnt,
        CONCAT(opened_at_ym, '#', CAST(cnt AS STRING)) AS opened_at_ym_ref,
        DATEDIFF(DATE '2025-06-30', last_closed_at) AS days_since_last_closed_at,
        DATE_TRUNC('QUARTER', last_closed_at) AS last_closed_at_quarter
    FROM per_group
    WHERE max_days_since_closed_at_rank < 6
)
SELECT
    opened_at_ym,
    last_closed_at,
    cnt,
    days_since_last_closed_at
FROM scored
ORDER BY opened_at_ym DESC, cnt NULLS LAST
