WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 32.9) AS score_distance
    FROM enrollments
    WHERE course_id >= 10
),
with_details_course AS (
    -- Join courses
    SELECT s.*, c.dept AS course_dept
    FROM filtered s
    LEFT JOIN courses c ON c.id = s.course_id
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        score - score_distance AS score_minus_score_distance,
        LENGTH(TRIM(term)) AS term_len
    FROM with_details_course
    WHERE grade_status IN ('FAIL', 'INCOMPLETE', 'PASS')
),
grouped AS (
    -- Group-level totals
    SELECT
        course_dept,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        student_id,
        COUNT(DISTINCT grade_status) AS distinct_grade_status,
        COUNT(DISTINCT term) AS distinct_terms,
        MIN(score_distance) AS min_score_distance,
        COUNT(*) AS n_rows,
        SUM(score_minus_score_distance) AS total_score_minus_score_distance
    FROM derived
    GROUP BY course_dept, DATE_TRUNC('MONTH', enrolled_at), student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.student_id) AS avg_enrollment_score
    FROM grouped s
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        course_dept,
        enrolled_at_month,
        student_id,
        n_rows,
        enrollment_count,
        avg_enrollment_score,
        COUNT(*) OVER (PARTITION BY n_rows) AS rows_in_n_rows,
        SIZE(COLLECT_SET(course_dept) OVER w_n_rows) AS distinct_course_dept_in_n_rows,
        n_rows - LAG(n_rows) OVER (PARTITION BY distinct_grade_status, student_id ORDER BY avg_enrollment_score DESC NULLS FIRST, course_dept, enrolled_at_month, student_id) AS n_rows_change
    FROM summarized_rows
    WINDOW w_n_rows AS (PARTITION BY n_rows)
)
SELECT
    course_dept,
    enrolled_at_month,
    student_id,
    n_rows,
    avg_enrollment_score,
    rows_in_n_rows,
    distinct_course_dept_in_n_rows,
    n_rows_change,
    DATE_FORMAT(enrolled_at_month, 'MMM yyyy') AS enrolled_at_month_label,
    ROUND(avg_enrollment_score * 1.07, 2) AS avg_enrollment_score_adj
FROM with_trends
WHERE n_rows > 2
ORDER BY enrolled_at_month NULLS LAST, course_dept, student_id
LIMIT 5
