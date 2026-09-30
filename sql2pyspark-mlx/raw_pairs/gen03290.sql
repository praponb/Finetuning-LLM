WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        level,
        CASE WHEN level IN ('L5', 'L2') THEN 'Core' ELSE 'Other' END level_group,
        COALESCE(level, 'NONE') resolved_level
    FROM employees
    WHERE hire_date >= DATE '2023-06-20'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        email,
        dept_id,
        hire_date,
        level,
        level_group,
        resolved_level,
        LAG(salary, 2) OVER (PARTITION BY level ORDER BY salary, id) prev_salary,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary, id) row_seq
    FROM filtered
    WHERE level = 'L1'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN level IN ('L4', 'L3') THEN 'Tier 1' ELSE 'Other' END level_group_2,
        row_seq + prev_salary row_seq_plus_prev_salary
    FROM enriched
    WHERE level IS NOT DISTINCT FROM 'L2'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        full_name,
        dept_id,
        hire_date,
        level,
        level_group,
        resolved_level,
        row_seq,
        level_group_2
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY row_seq_plus_prev_salary DESC, id) AS dedup_rank
        FROM base
    ) ranked
    WHERE dedup_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(row_seq) OVER (PARTITION BY level ORDER BY row_seq, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_min_row_seq,
        AVG(row_seq) OVER (PARTITION BY level ORDER BY row_seq, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_avg_row_seq,
        SUM(row_seq) OVER (PARTITION BY level ORDER BY row_seq, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_sum_row_seq,
        COUNT(row_seq) OVER (PARTITION BY level ORDER BY row_seq, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_row_seq
    FROM latest
),
kept_keys AS (
    -- Set difference on keys
    SELECT level, id
    FROM scored
    EXCEPT ALL
    SELECT level, id
    FROM employees
    WHERE email IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN kept_keys k ON s.level = k.level AND s.id = k.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        dept_id,
        level,
        row_seq
    FROM verified
) src
PIVOT (
    MAX(row_seq) AS total,
    COUNT(1) AS cnt
    FOR level IN ('L2', 'L4')
)
