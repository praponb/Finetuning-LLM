WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        location,
        budget,
        COUNT(budget) OVER (PARTITION BY location) AS location_count_budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share,
        NTILE(3) OVER (PARTITION BY dept_name ORDER BY budget, id) AS budget_bucket,
        AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS budget_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget, id) AS seq_no
    FROM departments
    WHERE location IN ('Berlin', 'Bangkok', 'Singapore')
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        budget - budget_avg_6_prior AS budget_minus_budget_avg_6_prior,
        CASE WHEN budget >= 3530000 THEN 'High' WHEN budget >= 1658000 THEN 'Medium' ELSE 'Low' END AS budget_band,
        COALESCE(location, 'N/A') AS location_clean
    FROM with_windows
    WHERE budget >= 3873000
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.level,
        top1.employee_hire_date
    FROM filtered s
    JOIN LATERAL (
            SELECT c.level, c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        location,
        location_count_budget,
        budget_bucket,
        budget_avg_6_prior,
        seq_no,
        budget_minus_budget_avg_6_prior,
        budget_band,
        location_clean,
        level
    FROM with_latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.full_name LIKE '%Emm%'
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        location,
        location_count_budget,
        budget_bucket,
        budget_avg_6_prior,
        seq_no,
        budget_band,
        ROW_NUMBER() OVER (PARTITION BY level, location ORDER BY seq_no DESC, id) AS event_seq,
        ROUND(budget_bucket / SUM(budget_bucket) OVER (PARTITION BY level, location), 4) AS budget_bucket_share,
        NTILE(3) OVER (PARTITION BY location ORDER BY seq_no, id) AS seq_no_bucket
    FROM kept
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        location_count_budget,
        budget_bucket,
        seq_no,
        budget_band,
        budget_bucket_share,
        DENSE_RANK() OVER (PARTITION BY location ORDER BY seq_no_bucket) AS seq_no_bucket_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY seq_no, id) AS row_seq
    FROM metrics
)
-- Aggregate with conditional metrics
SELECT
    seq_no,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(budget_bucket) AS avg_budget_bucket,
    AVG(seq_no) AS avg_seq_no,
    PERCENTILE_APPROX(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
    ROUND(AVG(seq_no), 2) AS avg_seq_no_2
FROM scored
GROUP BY seq_no
ORDER BY seq_no DESC;
