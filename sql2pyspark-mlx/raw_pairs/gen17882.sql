WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(budget * 0.85, 2) AS budget_adjusted,
        CEIL(budget) AS budget_whole
    FROM departments
    WHERE budget BETWEEN 1875000 AND 3956000
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name, location), 4) AS budget_share,
        SUM(budget_whole) OVER w_running AS cumulative_budget_whole,
        AVG(budget_whole) OVER (ORDER BY budget ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS budget_whole_avg_3
    FROM cleaned
    WHERE budget_whole <= 57
    WINDOW w_running AS (PARTITION BY dept_name ORDER BY budget_whole ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY location ORDER BY cumulative_budget_whole ASC) AS row_num
    FROM enriched
    QUALIFY row_num = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        location,
        budget_adjusted,
        budget_whole,
        budget_share,
        budget_whole_avg_3,
        row_num,
        ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget_whole ASC, id ASC) AS row_seq,
        SUM(budget_adjusted) OVER (PARTITION BY location ORDER BY budget_adjusted ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget_adjusted
    FROM deduped
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        location,
        dept_name,
        budget_share
    FROM metrics
) src
PIVOT (
    AVG(budget_share)
    FOR dept_name IN ('HR', 'Legal', 'Marketing')
);
