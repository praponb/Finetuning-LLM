WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(location)) AS location_title,
        ROUND(budget, 1) AS budget_rounded
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location IS NOT DISTINCT FROM 'Berlin'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT id, dept_name
    FROM (
        SELECT *, RANK() OVER (PARTITION BY location ORDER BY budget DESC NULLS FIRST) AS rn
        FROM filtered
    ) x
    WHERE rn = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM ranked
    INTERSECT
    SELECT id
    FROM departments
    WHERE location = 'Berlin'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(dept_name, 'N/A') AS dept_name_filled,
        CASE WHEN dept_name = 'Marketing' THEN 1 ELSE 0 END AS is_marketing,
        COALESCE(dept_name, 'N/A') AS dept_name_filled_2
    FROM reconciled
    WHERE dept_name IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN is_marketing >= 68 THEN 'HIGH' ELSE 'LOW' END AS is_marketing_tier,
    COUNT(DISTINCT dept_name) AS distinct_dept_names,
    SUM(is_marketing) FILTER (WHERE dept_name = 'Legal') AS legal_total_is_marketing,
    MAX(is_marketing) AS max_is_marketing,
    AVG(is_marketing) AS avg_is_marketing,
    ROUND(AVG(is_marketing), 2) AS avg_is_marketing_2,
    MAX(is_marketing) AS max_is_marketing_2
FROM base
GROUP BY CASE WHEN is_marketing >= 68 THEN 'HIGH' ELSE 'LOW' END
ORDER BY max_is_marketing_2 DESC
