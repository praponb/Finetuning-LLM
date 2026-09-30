WITH scored AS (
    SELECT
        id,
        budget,
        LAG(budget) OVER w_running AS prev_budget,
        LEAD(budget) OVER w_running AS next_budget,
        ROW_NUMBER() OVER w_running AS seq_no
    FROM departments
    WHERE budget BETWEEN 3267000 AND 3316000
    WINDOW w_running AS (PARTITION BY dept_name, location ORDER BY budget, id)
),
scored_departments AS (
    SELECT
        id,
        budget,
        prev_budget,
        LEAD(prev_budget, 1) OVER (ORDER BY prev_budget, id) AS next_prev_budget,
        LAG(next_budget, 2) OVER (ORDER BY next_budget, id) AS prev_next_budget,
        ROW_NUMBER() OVER (ORDER BY prev_budget, id) AS event_seq
    FROM scored
),
filtered AS (
    SELECT
        id,
        budget,
        prev_budget,
        event_seq,
        CAST(prev_next_budget AS INT) AS prev_next_budget_int
    FROM scored_departments
    WHERE next_prev_budget >= 2560000
),
base AS (
    SELECT
        id,
        budget,
        prev_budget,
        CAST(prev_next_budget_int AS DOUBLE) AS prev_next_budget_int_str
    FROM filtered
    WHERE event_seq BETWEEN 6 AND 7
),
kept AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.is_remote
        )
),
kept_keys AS (
    SELECT id
    FROM kept
    INTERSECT ALL
    SELECT id
    FROM departments
    WHERE location NOT IN ('Tokyo', 'Berlin', 'New York')
),
reconciled AS (
    SELECT s.*
    FROM kept s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    CASE WHEN budget >= 3701000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
    ROUND(AVG(budget), 2) AS avg_budget,
    MAX(prev_budget) AS max_prev_budget
FROM reconciled
GROUP BY 1
ORDER BY avg_budget DESC NULLS FIRST
