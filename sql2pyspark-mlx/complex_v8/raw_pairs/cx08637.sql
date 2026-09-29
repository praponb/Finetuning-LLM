WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        CASE WHEN grade_status IN ('FAIL', 'PASS') THEN 'group_a' WHEN grade_status = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-24 09:00:00'
),
grouped AS (
    -- Summarise per group
    SELECT
        grade_status_group,
        ROUND(STDDEV(score), 2) AS stddev_score,
        ROUND(SUM(score) / NULLIF(SUM(grade_status_len), 0), 3) AS score_to_grade_status_len_ratio,
        MIN(enrolled_at) AS first_enrolled_at,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        PERCENTILE_APPROX(score, 0.5) AS median_score,
        COUNT(*) AS n_rows
    FROM filtered_enrollments
    WHERE grade_status_len < 12
    GROUP BY 1
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM grouped
    WHERE median_score > (SELECT AVG(median_score) FROM grouped)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(distinct_student_ids) OVER (PARTITION BY grade_status_group) AS grade_status_group_max_distinct_student_ids,
        distinct_student_ids - LAG(distinct_student_ids) OVER (ORDER BY stddev_score, grade_status_group) AS distinct_student_ids_change,
        MAX(median_score) OVER (PARTITION BY grade_status_group) AS grade_status_group_max_median_score,
        MIN(median_score) OVER w_grade_status_group AS grade_status_group_min_median_score,
        ROUND(n_rows - AVG(n_rows) OVER w_grade_status_group, 2) AS n_rows_vs_grade_status_group_avg
    FROM filtered
    WINDOW w_grade_status_group AS (PARTITION BY grade_status_group)
)
SELECT *
FROM (
    SELECT distinct_student_ids, grade_status_group, grade_status_group_max_median_score
    FROM ranked
) src
PIVOT (
    SUM(grade_status_group_max_median_score)
    FOR grade_status_group IN ('other', 'group_a', 'group_b')
)
ORDER BY distinct_student_ids
