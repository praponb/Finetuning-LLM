WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(budget) OVER w_running AS next_budget,
        LAG(budget) OVER w_running AS prev_budget,
        ROW_NUMBER() OVER w_running AS event_seq
    FROM departments
    WINDOW w_running AS (PARTITION BY location ORDER BY budget, id)
),
metrics_departments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(event_seq) OVER w_recent AS last4_min_event_seq,
        SUM(event_seq) OVER w_recent AS last4_sum_event_seq,
        MAX(event_seq) OVER w_recent AS last4_max_event_seq
    FROM metrics
    WHERE event_seq <= 6
    WINDOW w_recent AS (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.employee_salary,
        top1.hire_date
    FROM metrics_departments s
    LEFT JOIN LATERAL (
            SELECT c.salary AS employee_salary, c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY hire_date NULLS LAST, id) = 1
),
metrics_departments_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        budget,
        prev_budget,
        event_seq,
        last4_max_event_seq,
        COUNT(next_budget) OVER (PARTITION BY dept_name ORDER BY last4_min_event_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_next_budget,
        AVG(next_budget) OVER (PARTITION BY dept_name ORDER BY last4_min_event_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_next_budget,
        SUM(next_budget) OVER (PARTITION BY dept_name ORDER BY last4_min_event_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_next_budget,
        MAX(next_budget) OVER (PARTITION BY dept_name ORDER BY last4_min_event_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_next_budget
    FROM deduped
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(prev_budget) OVER w_running AS running_min_prev_budget,
        COUNT(prev_budget) OVER w_running AS running_count_prev_budget,
        AVG(prev_budget) OVER w_running AS running_avg_prev_budget,
        MAX(prev_budget) OVER w_running AS running_max_prev_budget,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM metrics_departments_2
    WINDOW w_running AS (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY prev_budget DESC, id)
),
metrics_departments_3 AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(row_seq) OVER (ORDER BY last4_max_next_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_row_seq,
        MAX(row_seq) OVER (ORDER BY last4_max_next_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_row_seq,
        MIN(row_seq) OVER (ORDER BY last4_max_next_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_row_seq,
        SUM(row_seq) OVER (ORDER BY last4_max_next_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_row_seq,
        LAG(last4_avg_next_budget, 2, 0) OVER (ORDER BY running_min_prev_budget, id) AS prev_last4_avg_next_budget
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_sum_row_seq >= 33 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_sum_row_seq_tier,
    ROUND(AVG(event_seq), 2) AS avg_event_seq,
    ROUND(AVG(prev_budget), 2) AS avg_prev_budget,
    PERCENTILE_APPROX(running_max_row_seq, 0.95) AS running_max_row_seq_p95
FROM metrics_departments_3
GROUP BY CASE WHEN running_sum_row_seq >= 33 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY running_sum_row_seq_tier DESC;
