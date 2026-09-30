WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        tier,
        hired_on
    FROM support_agents
    WHERE tier NOT IN ('T1', 'T3')
      AND hired_on >= DATE '2024-05-02'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY tier ORDER BY hired_on, id) AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY tier ORDER BY hired_on), 3) AS hired_on_cume_dist,
        DATEDIFF(hired_on, LAG(hired_on) OVER w_tier_by_hired_on) AS days_since_prev_hired_on,
        ROUND(PERCENT_RANK() OVER (PARTITION BY tier ORDER BY hired_on, id), 3) AS hired_on_percent_rank,
        LEAD(hired_on) OVER w_tier_by_hired_on AS next_hired_on,
        FIRST_VALUE(hired_on) IGNORE NULLS OVER (PARTITION BY tier ORDER BY hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_hired_on,
        LAST_VALUE(hired_on) IGNORE NULLS OVER (PARTITION BY tier ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_hired_on
    FROM valid
    WINDOW w_tier_by_hired_on_ties AS (PARTITION BY tier ORDER BY hired_on), w_tier_by_hired_on AS (PARTITION BY tier ORDER BY hired_on, id)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        YEAR(last_known_hired_on) AS last_known_hired_on_year,
        DATE_FORMAT(hired_on, 'yyyy-MM') AS hired_on_label,
        ROUND(hired_on_percent_rank * 1.5, 2) AS hired_on_percent_rank_adj
    FROM with_trends
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(days_since_prev_hired_on - AVG(days_since_prev_hired_on) OVER (PARTITION BY last_known_hired_on_year), 2) AS days_since_prev_hired_on_vs_last_known_hired_on_,
        ROUND(days_since_prev_hired_on / NULLIF(SUM(days_since_prev_hired_on) OVER w_seq_no_last_known_hired_on_y, 0), 4) AS days_since_prev_hired_on_share_of_seq_no_last_kn,
        ROUND(last_known_hired_on_year - AVG(last_known_hired_on_year) OVER (PARTITION BY last_known_hired_on_year), 2) AS last_known_hired_on_year_vs_last_known_hired_on_,
        LAST_VALUE(days_since_prev_hired_on) IGNORE NULLS OVER (PARTITION BY tier, DATE_TRUNC('MONTH', hired_on) ORDER BY hired_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_prev_hired_on,
        MAX(seq_no) OVER w_seq_no_last_known_hired_on_y AS seq_no_last_known_hired_on_year_max_seq_no,
        NTH_VALUE(hired_on_cume_dist, 2) OVER (PARTITION BY last_known_hired_on_year ORDER BY hired_on_percent_rank_adj NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on_cume_dist
    FROM enriched
    WINDOW w_seq_no_last_known_hired_on_y AS (PARTITION BY seq_no, last_known_hired_on_year)
)
SELECT
    id,
    agent_name,
    tier,
    hired_on,
    hired_on_cume_dist,
    days_since_prev_hired_on,
    first_known_hired_on,
    last_known_hired_on,
    last_known_hired_on_year,
    hired_on_label,
    hired_on_percent_rank_adj,
    days_since_prev_hired_on_share_of_seq_no_last_kn,
    last_known_hired_on_year_vs_last_known_hired_on_,
    last_known_days_since_prev_hired_on,
    seq_no_last_known_hired_on_year_max_seq_no,
    second_hired_on_cume_dist,
    ABS(seq_no - 5) AS seq_no_distance,
    CONCAT(agent_name, '#', CAST(last_known_hired_on_year AS STRING)) AS agent_name_ref
FROM ranked
WHERE days_since_prev_hired_on <= 160
ORDER BY hired_on, agent_name
