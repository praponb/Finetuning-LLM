WITH cleaned AS (
    SELECT
        id,
        job_title,
        hire_date,
        is_remote,
        level,
        ADD_MONTHS(hire_date, 3) AS hire_date_plus_months,
        COALESCE(job_title, 'N/A') AS resolved_job_title
    FROM employees
    WHERE hire_date > TO_DATE('2023-04-17')
      AND level IS NOT NULL
      AND email LIKE '%@gmail.com'
),
with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY hire_date DESC, id ASC) AS row_seq,
        LAST_VALUE(job_title, TRUE) OVER (PARTITION BY level, job_title ORDER BY hire_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title,
        FIRST_VALUE(job_title) IGNORE NULLS OVER (ORDER BY hire_date_plus_months ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title
    FROM cleaned
),
scored AS (
    SELECT
        id,
        job_title,
        hire_date,
        is_remote,
        level,
        hire_date_plus_months,
        resolved_job_title,
        row_seq,
        last_known_job_title,
        first_known_job_title,
        COUNT(row_seq) OVER (PARTITION BY level) AS level_count_row_seq,
        AVG(row_seq) OVER (ORDER BY hire_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS row_seq_avg_2_prior
    FROM with_windows
)
SELECT
    CASE WHEN level_count_row_seq >= 15 THEN 'HIGH' ELSE 'LOW' END AS level_count_row_seq_tier,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles,
    SIZE(COLLECT_SET(level)) AS num_levels,
    COUNT(*) FILTER (WHERE level = 'L3') AS l3_rows,
    SIZE(COLLECT_SET(level)) AS num_levels_2,
    SUM(CASE WHEN level = 'L5' THEN row_seq_avg_2_prior ELSE 0 END) AS l5_row_seq_avg_2_prior,
    GROUPING_ID() AS grouping_id
FROM scored
GROUP BY CUBE (CASE WHEN level_count_row_seq >= 15 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY num_levels_2 DESC
