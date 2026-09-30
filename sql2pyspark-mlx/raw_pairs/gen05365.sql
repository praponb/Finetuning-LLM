WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(full_name) AS full_name_length
    FROM employees
    WHERE hire_date > DATE '2023-05-07'
      AND level IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary DESC, id) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY latest_rank, id) AS rn
        FROM deduped
    ) x
    WHERE rn = 1
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        s.id,
        s.dept_id,
        s.is_remote,
        s.level,
        s.full_name_length,
        s.rn,
        d.dept_name,
        d.budget,
        d.location
    FROM ranked s
    LEFT JOIN departments d
        ON s.dept_id = d.id
    WHERE d.budget > 2858000
)
-- Multi-level aggregation
SELECT
    dept_id,
    COUNT_IF(level = 'L2') AS l2_events,
    SUM(full_name_length) FILTER (WHERE level = 'L1') AS l1_total_full_name_length,
    COUNT(CASE WHEN location = 'Singapore' THEN 1 END) AS singapore_count,
    MAX(full_name_length) FILTER (WHERE level = 'L1') AS l1_max_full_name_length,
    COUNT(DISTINCT dept_id) AS distinct_dept_ids
FROM with_dims
GROUP BY dept_id
HAVING COUNT(CASE WHEN location = 'Singapore' THEN 1 END) > 1;
