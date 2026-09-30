WITH prepared_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        csat,
        tags
    FROM tickets
    WHERE csat < 5
      AND agent_id IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        closed_at,
        csat,
        tags,
        NTH_VALUE(closed_at, 2) OVER (ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        COUNT(*) OVER (PARTITION BY MONTH(closed_at)) AS rows_in_closed_at_month,
        NTILE(4) OVER (PARTITION BY csat, agent_id ORDER BY csat DESC, id) AS csat_quartile,
        MAX(csat) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at), agent_id) AS closed_at_month_agent_id_max_csat,
        FIRST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY csat ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat,
        ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY csat, id) AS rn,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY MONTH(closed_at)), 0), 4) AS csat_share_of_closed_at_month,
        DENSE_RANK() OVER (PARTITION BY agent_id ORDER BY closed_at ASC, id) AS closed_at_dense_rank,
        ROUND(AVG(csat) OVER (PARTITION BY agent_id ORDER BY csat, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS csat_avg_3,
        FIRST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat_2,
        MIN(csat) OVER (PARTITION BY YEAR(closed_at), agent_id) AS closed_at_year_agent_id_min_csat
    FROM prepared_tickets
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        csat_quartile,
        YEAR(closed_at) AS closed_at_year,
        ROUND(STDDEV(rows_in_closed_at_month), 2) AS stddev_rows_in_closed_at_month,
        SUM(csat_share_of_closed_at_month) AS total_csat_share_of_closed_at_month,
        SUM(closed_at_month_agent_id_max_csat) AS total_closed_at_month_agent_id_max_csat,
        MIN(second_closed_at) AS first_second_closed_at
    FROM metrics
    GROUP BY csat_quartile, YEAR(closed_at)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        csat_quartile,
        closed_at_year,
        stddev_rows_in_closed_at_month,
        total_csat_share_of_closed_at_month,
        total_closed_at_month_agent_id_max_csat,
        first_second_closed_at,
        NTILE(3) OVER (PARTITION BY MONTH(first_second_closed_at) ORDER BY stddev_rows_in_closed_at_month, csat_quartile, closed_at_year) AS stddev_rows_in_closed_at_month_tercile,
        COUNT(*) OVER (PARTITION BY MONTH(first_second_closed_at) ORDER BY stddev_rows_in_closed_at_month, csat_quartile, closed_at_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM totals
)
-- Final projection
SELECT
    csat_quartile,
    closed_at_year,
    stddev_rows_in_closed_at_month,
    first_second_closed_at,
    stddev_rows_in_closed_at_month_tercile,
    running_count
FROM ranked
ORDER BY first_second_closed_at, csat_quartile, closed_at_year
LIMIT 10
