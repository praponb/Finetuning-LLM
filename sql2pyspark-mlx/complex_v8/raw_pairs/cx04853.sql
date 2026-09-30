WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label,
        CASE WHEN score >= 79.2 THEN 'large' WHEN score >= 13.9 THEN 'medium' ELSE 'small' END AS score_tier
    FROM enrollments
    WHERE grade_status IN ('PASS', 'INCOMPLETE', 'WITHDRAWN')
),
combined_details_student AS (
    -- Enrich with dimension data
    SELECT s.*, st.major AS student_major, st.student_name
    FROM filtered_enrollments s
    JOIN students st ON st.id = s.student_id
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        term,
        score,
        grade_status,
        enrolled_at,
        grade_status_or_unknown,
        enrolled_at_label,
        score_tier,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label_2,
        enrolled_at + INTERVAL 30 MINUTES AS enrolled_at_plus_30m,
        ROUND(score * 1.07, 2) AS score_adj
    FROM combined_details_student
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.term,
        s.grade_status,
        s.grade_status_or_unknown,
        s.enrolled_at_label,
        s.score_tier,
        s.enrolled_at_plus_30m,
        s.score_adj,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.score AS enrollment_score
    FROM annotated s
    JOIN enrollments e ON e.student_id = s.student_id AND e.score >= 30.0
)
-- Subtotals with ROLLUP
SELECT
    student_id,
    COALESCE(enrollment_term, 'ALL') AS enrollment_term,
    COALESCE(grade_status, 'ALL') AS grade_status,
    ROUND(SUM(enrollment_score), 2) AS total_enrollment_score
FROM exploded_detail
GROUP BY ROLLUP(student_id, enrollment_term, grade_status)
