WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date, id) AS rank_in_group
    FROM employees
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    full_name,
    dept_id,
    salary,
    is_remote,
    level,
    rank_in_group,
    CASE WHEN full_name RLIKE '^(Mia Rossi|Emma Brown)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS full_name_zone
FROM deduped
