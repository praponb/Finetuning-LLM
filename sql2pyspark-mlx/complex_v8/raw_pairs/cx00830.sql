WITH base_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        DATE_TRUNC('MONTH', closed_at) AS closed_at_month,
        DATE_FORMAT(opened_at, 'MMM yyyy') AS opened_at_label,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-02-14 22:00:00'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.closed_at_month,
        s.priority_or_unknown,
        su.tier AS agent_tier,
        su.team AS agent_team,
        su.agent_name
    FROM base_tickets s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE LOWER(su.agent_name) LIKE '%on%'
),
latest AS (
    -- Keep the latest row per priority_or_unknown
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        closed_at_month,
        priority_or_unknown,
        agent_tier,
        agent_team,
        agent_name
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority_or_unknown ORDER BY closed_at DESC, id DESC) AS row_num
        FROM joined
    ) ranked
    WHERE row_num = 1
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', closed_at_month) AS closed_at_month_month,
        YEAR(closed_at) AS closed_at_year,
        agent_tier,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(csat, 0.5) AS median_csat
    FROM latest
    GROUP BY DATE_TRUNC('MONTH', closed_at_month), YEAR(closed_at), agent_tier
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY row_count NULLS LAST, closed_at_month_month, closed_at_year, agent_tier) AS row_count_dense_rank,
        NTH_VALUE(closed_at_year, 2) OVER (ORDER BY row_count NULLS LAST, closed_at_month_month, closed_at_year, agent_tier ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at_year,
        ROUND(closed_at_year / NULLIF(SUM(closed_at_year) OVER (PARTITION BY row_count, median_csat), 0), 4) AS closed_at_year_share_of_row_count_median_csat,
        ROUND(AVG(median_csat) OVER w_row_count_median_csat, 2) AS row_count_median_csat_avg_median_csat,
        ROUND(row_count / NULLIF(SUM(row_count) OVER (PARTITION BY row_count, median_csat), 0), 4) AS row_count_share_of_row_count_median_csat
    FROM per_group
    WINDOW w_row_count_median_csat AS (PARTITION BY row_count, median_csat)
)
SELECT
    closed_at_month_month,
    row_count,
    row_count_dense_rank,
    second_closed_at_year,
    closed_at_year_share_of_row_count_median_csat,
    row_count_median_csat_avg_median_csat,
    row_count_share_of_row_count_median_csat,
    DATE_TRUNC('MONTH', closed_at_month_month) AS closed_at_month_month_month,
    DATE_FORMAT(closed_at_month_month, 'yyyy-MM-dd') AS closed_at_month_month_label
FROM windowed
ORDER BY row_count_median_csat_avg_median_csat DESC NULLS FIRST
