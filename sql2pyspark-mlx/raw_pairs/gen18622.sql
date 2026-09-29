WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN budget >= 3429000 THEN 'High' WHEN budget >= 881000 THEN 'Medium' ELSE 'Low' END AS budget_tier,
        ROUND(budget * 100, 2) AS budget_adjusted
    FROM departments
    WHERE location = 'Chiang Mai'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        location,
        budget_tier,
        budget_adjusted,
        LEAD(budget) OVER w_running AS next_budget,
        LEAD(budget_adjusted) OVER w_running AS next_budget_adjusted,
        LAG(budget_adjusted) OVER w_running AS prev_budget_adjusted,
        ROW_NUMBER() OVER w_running AS row_seq,
        MIN(budget_adjusted) OVER (PARTITION BY dept_name, location) AS dept_name_min_budget_adjusted
    FROM valid
    WINDOW w_running AS (PARTITION BY dept_name ORDER BY budget_adjusted, id)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        location,
        budget_tier,
        budget_adjusted,
        next_budget_adjusted,
        prev_budget_adjusted,
        row_seq,
        SUBSTRING(location, 1, 2) AS location_prefix,
        GREATEST(dept_name_min_budget_adjusted, prev_budget_adjusted) AS greatest_dept_name_min_budget_adjusted_prev_budget_adjusted,
        COALESCE(location, 'N/A') AS resolved_location
    FROM metrics
    WHERE location IS NOT NULL
      AND next_budget <= 3042000
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, location
    FROM base
    EXCEPT
    SELECT id, location
    FROM departments
    WHERE budget BETWEEN 2630000 AND 2760000
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.id = k.id AND s.location = k.location
),
enriched AS (
    -- Ranking and gap features
    SELECT *, AVG(row_seq) OVER (PARTITION BY location) AS location_avg_row_seq
    FROM verified
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        location,
        next_budget_adjusted,
        row_seq,
        greatest_dept_name_min_budget_adjusted_prev_budget_adjusted,
        resolved_location,
        location_avg_row_seq,
        FLOOR(greatest_dept_name_min_budget_adjusted_prev_budget_adjusted) AS greatest_dept_name_min_budget_adjusted_prev_budget_adjusted_whole
    FROM enriched
    WHERE location IS NOT NULL
      AND location IS DISTINCT FROM 'London'
)
SELECT
    location,
    next_budget_adjusted,
    row_seq,
    greatest_dept_name_min_budget_adjusted_prev_budget_adjusted_whole,
    CASE WHEN location RLIKE '^(London|Chiang Mai)$' THEN 'GROUP_B' WHEN location RLIKE '^(Bangkok|New York)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS location_zone
FROM cleaned;
