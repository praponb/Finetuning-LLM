WITH cleaned_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter
    FROM enrollments
    WHERE score BETWEEN 20.2 AND 78.8
      AND student_id IS NOT NULL
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        course_id,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE term = '2024T2') AS n_term_2024t2,
        ROUND(SUM(score), 2) AS total_score,
        SUM(CASE WHEN grade_status = 'WITHDRAWN' THEN score ELSE 0 END) AS score_grade_status_withdrawn,
        SUM(CASE WHEN term NOT IN ('2025T2', '2024T1') THEN score ELSE 0 END) AS score_term_not_in_2025t2_2024t1,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list
    FROM cleaned_enrollments
    WHERE student_id IS NOT NULL
    GROUP BY 1
    HAVING MAX(score) > 10.8
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(n_term_2024t2, 0) AS n_term_2024t2_filled,
        ARRAY_JOIN(ARRAY_SORT(grade_status_list), ',') AS grade_status_list_text,
        ROUND(row_count / NULLIF(total_score, 0), 3) AS row_count_per_total_score
    FROM grouped
    WHERE score_grade_status_withdrawn BETWEEN 150.0 AND 300.0
),
with_parent AS (
    -- Attach course attributes
    SELECT s.*, c.course_code, c.level AS course_level
    FROM tagged s
    JOIN courses c ON c.id = s.course_id
),
with_parent_course AS (
    -- Join courses
    SELECT
        s.course_id,
        s.total_score,
        s.score_grade_status_withdrawn,
        s.score_term_not_in_2025t2_2024t1,
        s.grade_status_list,
        s.grade_status_list_text,
        s.row_count_per_total_score,
        s.course_level,
        c.level AS course_level_2,
        c.credits AS course_credits,
        c.dept AS course_dept
    FROM with_parent s
    JOIN courses c ON c.id = s.course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS first_enrollment_enrolled_at,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS first_enrollment_enrolled_at_2
    FROM with_parent_course s
)
-- Final projection
SELECT
    course_id,
    score_grade_status_withdrawn,
    grade_status_list,
    grade_status_list_text,
    course_level,
    course_level_2,
    course_dept,
    first_enrollment_enrolled_at_2
FROM profiled
ORDER BY course_level_2 NULLS LAST, course_level
