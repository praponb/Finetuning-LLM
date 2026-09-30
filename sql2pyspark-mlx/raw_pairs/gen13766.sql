WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget DESC NULLS LAST, id) AS row_num
    FROM departments
    QUALIFY row_num = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.dept_id = 14
        )
),
eligible_departments AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.hire_date > '2023-12-10'
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        LEAD(budget, 1) OVER (PARTITION BY dept_name, location ORDER BY row_num, id) next_budget,
        NTILE(10) OVER (PARTITION BY dept_name, location ORDER BY budget DESC, id) budget_bucket,
        LAG(budget, 1) OVER w_ordered prev_budget,
        SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_budget
    FROM eligible_departments
    WINDOW w_ordered AS (PARTITION BY dept_name ORDER BY budget, id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        next_budget,
        budget_bucket,
        prev_budget,
        SUM(budget_bucket) OVER (PARTITION BY dept_name ORDER BY prev_budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_sum_budget_bucket,
        AVG(budget_bucket) OVER (PARTITION BY dept_name ORDER BY prev_budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_avg_budget_bucket,
        MIN(budget_bucket) OVER (PARTITION BY dept_name ORDER BY prev_budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_min_budget_bucket
    FROM scored
),
enriched_departments AS (
    -- Running and moving aggregates
    SELECT
        id,
        next_budget,
        budget_bucket,
        last4_sum_budget_bucket,
        CUME_DIST() OVER (ORDER BY last4_avg_budget_bucket NULLS LAST) last4_avg_budget_bucket_cume_dist,
        LAST_VALUE(last4_sum_budget_bucket, TRUE) OVER (ORDER BY prev_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_last4_sum_budget_bucket,
        ROW_NUMBER() OVER w_grp row_seq
    FROM enriched
    WINDOW w_grp AS (ORDER BY last4_sum_budget_bucket, id)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched_departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.email IS NOT NULL
        )
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN last4_avg_budget_bucket_cume_dist >= 0.86 THEN 'HIGH' ELSE 'LOW' END AS last4_avg_budget_bucket_cume_dist_tier,
        ROUND(AVG(last_known_last4_sum_budget_bucket), 2) AS avg_last_known_last4_sum_budget_bucket,
        PERCENTILE_APPROX(next_budget, 0.9) AS next_budget_p90
    FROM not_flagged
    GROUP BY last4_avg_budget_bucket_cume_dist_tier
)
SELECT last4_avg_budget_bucket_cume_dist_tier, next_budget_p90
FROM stats
ORDER BY next_budget_p90 DESC NULLS FIRST, last4_avg_budget_bucket_cume_dist_tier DESC;
