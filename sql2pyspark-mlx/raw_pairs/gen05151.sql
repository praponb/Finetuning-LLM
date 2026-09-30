WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.level = 'L1'
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(budget) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS row_seq
    FROM not_flagged
    WHERE dept_name <> 'Engineering'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share,
        ROW_NUMBER() OVER (ORDER BY row_seq, id) AS row_seq_2,
        AVG(row_seq) OVER (ORDER BY row_seq, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS row_seq_avg_6_prior,
        SUM(row_seq) OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_row_seq,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share_2
    FROM metrics
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        budget,
        row_seq,
        budget_share,
        row_seq_2,
        row_seq_avg_6_prior,
        cumulative_row_seq,
        AVG(prev_budget) OVER (PARTITION BY dept_name ORDER BY row_seq, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS prev_budget_avg_2_prior,
        LAST_VALUE(cumulative_row_seq, TRUE) OVER (ORDER BY row_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cumulative_row_seq,
        LAST_VALUE(budget_share_2) IGNORE NULLS OVER (PARTITION BY location, dept_name ORDER BY row_seq_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_budget_share_2
    FROM scored
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(row_seq_avg_6_prior, budget) AS greatest_row_seq_avg_6_prior_budget
    FROM enriched
    WHERE budget_share >= 0.68
)
SELECT
    id,
    dept_name,
    row_seq,
    row_seq_2,
    row_seq_avg_6_prior,
    cumulative_row_seq,
    last_known_budget_share_2,
    greatest_row_seq_avg_6_prior_budget,
    CASE WHEN dept_name RLIKE '^(Sales|Finance)$' THEN 'EXTENDED' WHEN dept_name RLIKE '^(Engineering|Marketing)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS dept_name_zone
FROM filtered
ORDER BY greatest_row_seq_avg_6_prior_budget NULLS FIRST, cumulative_row_seq DESC;
