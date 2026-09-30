WITH cleaned AS (
    SELECT
        *,
        INITCAP(TRIM(location)) AS location_title,
        COALESCE(location, 'DIRECT') AS location_filled
    FROM departments
    WHERE location IS NOT NULL
),
filtered AS (
    SELECT *, ROUND(budget, 1) AS budget_rounded
    FROM cleaned
    WHERE dept_name IS NOT NULL
),
filtered_departments AS (
    SELECT
        *,
        LEAST(budget, budget_rounded) AS least_budget_budget_rounded,
        CONCAT_WS('-', location, dept_name) AS location_dept_name_key,
        COALESCE(dept_name, 'DIRECT') AS resolved_dept_name
    FROM filtered
    WHERE dept_name IS NOT NULL
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY least_budget_budget_rounded DESC, id ASC) AS rn
        FROM filtered_departments
    ) x
    WHERE rn = 1
),
cleaned_departments AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        location_title,
        least_budget_budget_rounded,
        budget_rounded + least_budget_budget_rounded AS budget_rounded_plus_least_budget_budget_rounded,
        COALESCE(budget, 0) AS budget_filled,
        COALESCE(location, 'UNKNOWN') AS resolved_location
    FROM latest
    WHERE dept_name NOT IN ('Legal', 'Marketing')
),
stats AS (
    SELECT
        CASE WHEN least_budget_budget_rounded >= 83 THEN 'PREMIUM' ELSE 'STANDARD' END AS least_budget_budget_rounded_tier,
        ROUND(SUM(budget_filled) / NULLIF(COUNT(CASE WHEN dept_name = 'Legal' THEN 1 END), 0), 2) AS budget_filled_per_legal,
        APPROX_PERCENTILE(budget_filled, ARRAY(0.25, 0.5, 0.75)) AS budget_filled_quartiles,
        ROUND(SUM(budget_filled) / NULLIF(COUNT(CASE WHEN dept_name = 'Marketing' THEN 1 END), 0), 2) AS budget_filled_per_marketing
    FROM cleaned_departments
    GROUP BY CASE WHEN least_budget_budget_rounded >= 83 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING ROUND(SUM(budget_filled) / NULLIF(COUNT(CASE WHEN dept_name = 'Legal' THEN 1 END), 0), 2) > 0
)
SELECT
    least_budget_budget_rounded_tier,
    budget_filled_per_legal,
    budget_filled_per_marketing
FROM stats;
