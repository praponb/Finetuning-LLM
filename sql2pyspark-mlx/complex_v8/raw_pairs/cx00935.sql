WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 57.6 THEN 'large' WHEN score >= 36.6 THEN 'medium' ELSE 'small' END AS score_tier,
        FLOOR(score / 25) * 25 AS score_bucket,
        CASE WHEN score >= 71.0 THEN 'HIGH' WHEN score >= 32.9 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier_2
    FROM enrollments
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM valid_enrollments s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
with_parent_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.score_tier,
        s.score_bucket,
        s.score_tier_2,
        st.enrolled_on AS student_enrolled_on,
        st.is_international AS student_is_international
    FROM with_stats s
    LEFT JOIN students st ON st.id = s.student_id
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_parent_student s
    WHERE s.course_id IN (
            SELECT e.course_id
            FROM enrollments e
            WHERE e.term = '2025T2'
        )
      AND s.student_id <= 18
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score_tier_2,
    student_enrolled_on
FROM with_activity
WHERE score_bucket <= 80
ORDER BY course_id DESC, student_enrolled_on
