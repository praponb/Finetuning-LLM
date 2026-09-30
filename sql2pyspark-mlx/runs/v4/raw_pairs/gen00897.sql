WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(location) IGNORE NULLS OVER w_grp last_known_location,
        LAST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY location ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_dept_name
    FROM departments
    WINDOW w_grp AS (ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_departments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        location,
        budget,
        last_known_location,
        last_known_dept_name,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id ASC) seq_no,
        CUME_DIST() OVER (PARTITION BY dept_name ORDER BY budget ASC NULLS FIRST) budget_cume_dist,
        FIRST_VALUE(location) IGNORE NULLS OVER (PARTITION BY location ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_location
    FROM with_windows
),
with_windows_departments_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(seq_no, TRUE) OVER (ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_seq_no,
        AVG(budget_cume_dist) OVER (PARTITION BY dept_name ORDER BY seq_no ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) budget_cume_dist_avg_5_prior
    FROM with_windows_departments
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC NULLS FIRST, id ASC) AS latest_rank
    FROM with_windows_departments_2
    QUALIFY latest_rank = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        location,
        budget,
        last_known_location,
        last_known_dept_name,
        seq_no,
        budget_cume_dist,
        first_known_location,
        first_known_seq_no,
        budget_cume_dist_avg_5_prior,
        latest_rank,
        NTILE(5) OVER (PARTITION BY location ORDER BY budget_cume_dist DESC, id ASC) budget_cume_dist_bucket,
        SUM(seq_no) OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_seq_no,
        LAST_VALUE(location) IGNORE NULLS OVER (ORDER BY budget_cume_dist_avg_5_prior ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_location_2,
        AVG(first_known_seq_no) OVER (PARTITION BY dept_name ORDER BY budget_cume_dist ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) first_known_seq_no_avg_3_prior,
        LAST_VALUE(location, TRUE) OVER (PARTITION BY location ORDER BY budget_cume_dist_avg_5_prior ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_location_3
    FROM ranked
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(location, '/', dept_name) location_dept_name,
        COALESCE(dept_name, 'UNKNOWN') resolved_dept_name
    FROM scored
    WHERE dept_name IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_name,
        location,
        last_known_location,
        seq_no,
        budget_cume_dist,
        first_known_location,
        first_known_seq_no,
        budget_cume_dist_bucket,
        cumulative_seq_no,
        last_known_location_2,
        last_known_location_3,
        location_dept_name,
        resolved_dept_name,
        CEIL(cumulative_seq_no) cumulative_seq_no_whole
    FROM cleaned
    WHERE dept_name IS NOT NULL
      AND dept_name NOT IN ('Finance', 'Engineering', 'HR')
)
SELECT
    id,
    dept_name,
    location,
    last_known_location,
    budget_cume_dist,
    first_known_location,
    first_known_seq_no,
    budget_cume_dist_bucket,
    last_known_location_2
FROM filtered
ORDER BY location ASC NULLS FIRST
