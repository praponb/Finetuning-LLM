WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        COALESCE(credits, 0) AS credits_filled
    FROM courses
    WHERE credits >= 2
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.id) AS enrollment_count
    FROM prepared s
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM profiled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.student_id IS NOT NULL
        )
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_code,
        s.credits,
        s.credits_filled,
        s.enrollment_count,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status,
        e.student_id AS enrollment_student_id,
        e.term AS enrollment_term
    FROM filtered s
    LEFT JOIN enrollments e ON e.course_id = s.id AND e.score > 34.2
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_children s
    WHERE s.enrollment_student_id IN (
            SELECT e.student_id
            FROM enrollments e
            WHERE e.enrolled_at < TIMESTAMP '2025-02-18 10:00:00'
              AND e.score BETWEEN 17.4 AND 66.0
        )
),
per_group AS (
    -- Summarise per group
    SELECT
        enrollment_student_id,
        enrollment_count,
        credits_filled,
        SUM(CASE WHEN enrollment_term = '2025T1' THEN 1 ELSE 0 END) AS n_enrollment_term_2025t1,
        ROUND(100 * COUNT(CASE WHEN enrollment_term IN ('2024T2', '2024T1') THEN 1 END) / COUNT(*), 1) AS pct_enrollment_term_in_2024t2_20,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(enrollment_grade_status)) AS enrollment_grade_status_list,
        ARRAY_SORT(COLLECT_SET(course_code)) AS course_code_list,
        COUNT(DISTINCT enrollment_term) AS distinct_enrollment_terms
    FROM qualified
    WHERE enrollment_term IS NOT NULL
    GROUP BY enrollment_student_id, enrollment_count, credits_filled
    HAVING COUNT(*) >= 2
)
SELECT
    enrollment_student_id,
    enrollment_count,
    credits_filled,
    n_enrollment_term_2025t1,
    enrollment_grade_status_list,
    distinct_enrollment_terms,
    LEAST(cnt, credits_filled) AS least_cnt_credits_filled,
    ROUND(enrollment_count / NULLIF(n_enrollment_term_2025t1, 0), 3) AS enrollment_count_per_n_enrollment_term_2025t1
FROM per_group
WHERE credits_filled BETWEEN 1 AND 3
ORDER BY enrollment_count_per_n_enrollment_term_2025t1 DESC, n_enrollment_term_2025t1
