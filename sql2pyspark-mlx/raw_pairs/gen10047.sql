WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(location) AS location_length,
        CEIL(budget) AS budget_whole
    FROM departments
    WHERE location IS NOT NULL
      AND location = 'London'
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.hire_date,
        latest.employee_level
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.hire_date, c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY employee_level, dept_name ORDER BY location_length, id) AS dedup_rank
    FROM latest_match
    QUALIFY dedup_rank = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY employee_level, location ORDER BY budget_whole DESC NULLS LAST, id) AS rank_in_group
    FROM latest
    QUALIFY rank_in_group = 1
),
latest_match_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.location_length,
        s.budget_whole,
        s.hire_date,
        s.employee_level,
        s.dedup_rank,
        x.salary
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, location
    FROM latest_match_departments
    EXCEPT DISTINCT
    SELECT id, location
    FROM departments
    WHERE budget >= 2169000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match_departments s
    INNER JOIN matched_keys k ON s.id = k.id AND s.location = k.location
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.level != 'L4'
        )
)
SELECT
    id,
    location_length,
    hire_date,
    dedup_rank
FROM kept
ORDER BY location_length DESC, id;
