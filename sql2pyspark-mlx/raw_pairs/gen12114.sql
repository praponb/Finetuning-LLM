WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(location, 1, 3) AS location_prefix,
        COALESCE(location, 'UNKNOWN') AS location_filled
    FROM departments
    WHERE dept_name NOT IN ('Finance', 'Sales', 'Marketing')
      AND location IS NOT NULL
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.job_title,
        latest.employee_hire_date
    FROM valid s
    JOIN LATERAL (
            SELECT c.job_title, c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(budget) OVER (ORDER BY employee_hire_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget,
        SUM(budget) OVER (PARTITION BY MONTH(employee_hire_date), job_title ORDER BY employee_hire_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget_2,
        AVG(budget) OVER (PARTITION BY location) AS location_avg_budget,
        SUM(budget) OVER w_ordered AS running_budget,
        AVG(budget) OVER w_grp AS budget_avg_3_prior
    FROM latest_match
    WINDOW w_ordered AS (PARTITION BY dept_name ORDER BY employee_hire_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY MONTH(employee_hire_date), location ORDER BY employee_hire_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        location,
        location_filled,
        job_title,
        employee_hire_date,
        cumulative_budget,
        budget_avg_3_prior,
        CUME_DIST() OVER (ORDER BY budget DESC NULLS FIRST) AS budget_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY YEAR(employee_hire_date), location ORDER BY budget DESC, id ASC) AS row_seq,
        SUM(budget) OVER (PARTITION BY YEAR(employee_hire_date), dept_name ORDER BY cumulative_budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget_2
    FROM scored
),
scored_departments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(row_seq) OVER w_running AS running_sum_row_seq,
        COUNT(row_seq) OVER w_running AS running_count_row_seq,
        AVG(row_seq) OVER w_running AS running_avg_row_seq,
        MAX(row_seq) OVER w_running AS running_max_row_seq
    FROM enriched
    WHERE row_seq = 8
    WINDOW w_running AS (PARTITION BY location ORDER BY employee_hire_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            job_title,
            location,
            budget_avg_3_prior
        FROM scored_departments
    ) src
    PIVOT (
        MAX(budget_avg_3_prior)
        FOR location IN ('Singapore' AS singapore, 'Berlin' AS berlin)
    )
)
SELECT
    job_title,
    singapore,
    berlin
FROM by_type
ORDER BY singapore ASC;
