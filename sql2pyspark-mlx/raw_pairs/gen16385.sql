WITH valid AS (
    SELECT *, COALESCE(dept_name, 'N/A') AS dept_name_filled
    FROM departments
    WHERE dept_name IS NOT DISTINCT FROM 'Finance'
),
cleaned AS (
    SELECT
        *,
        LENGTH(dept_name) AS dept_name_length,
        COALESCE(budget, 0) AS budget_filled,
        COALESCE(location, 'N/A') AS resolved_location
    FROM valid
    WHERE budget > 2313000
),
scored AS (
    SELECT
        id,
        dept_name,
        location,
        budget_filled,
        STDDEV_SAMP(budget) OVER (PARTITION BY dept_name, location) AS dept_name_stddev_samp_budget,
        AVG(budget) OVER (ORDER BY dept_name_length ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS budget_avg_2_prior,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share,
        AVG(dept_name_length) OVER (ORDER BY budget_filled ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS dept_name_length_avg_5,
        AVG(budget) OVER (PARTITION BY location) AS location_avg_budget
    FROM cleaned
),
metrics AS (
    SELECT
        id,
        dept_name,
        location,
        budget_filled,
        dept_name_stddev_samp_budget,
        budget_avg_2_prior,
        budget_share,
        location_avg_budget,
        MIN(dept_name_length_avg_5) OVER (PARTITION BY location) AS location_min_dept_name_length_avg_5,
        AVG(budget_avg_2_prior) OVER (PARTITION BY dept_name ORDER BY location_avg_budget ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS budget_avg_2_prior_avg_3_prior
    FROM scored
),
kept_keys AS (
    SELECT dept_name, location
    FROM metrics
    EXCEPT ALL
    SELECT dept_name, location
    FROM departments
    WHERE location != 'Tokyo'
),
reconciled AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.dept_name = k.dept_name AND s.location = k.location
)
SELECT
    id,
    dept_name,
    budget_avg_2_prior,
    budget_share,
    location_avg_budget,
    location_min_dept_name_length_avg_5,
    budget_avg_2_prior_avg_3_prior
FROM reconciled
ORDER BY budget_share ASC;
