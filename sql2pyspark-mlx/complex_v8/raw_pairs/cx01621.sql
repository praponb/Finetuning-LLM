WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CASE WHEN score >= 80.2 THEN 'HIGH' WHEN score >= 16.3 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier
    FROM enrollments
    WHERE score <= 63.9
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        course_id,
        SUM(score) FILTER (WHERE term IS NOT NULL) AS score_term_is_not_null,
        SUM(score) AS total_score,
        COUNT(*) FILTER (WHERE grade_status = 'WITHDRAWN') AS n_grade_status_withdrawn,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list
    FROM valid_enrollments
    GROUP BY course_id
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.course_id,
        s.total_score,
        s.n_grade_status_withdrawn,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.enrolled_at AS enrollment_enrolled_at,
        e.student_id AS enrollment_student_id,
        e.term AS enrollment_term
    FROM summary s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
)
-- Final projection
SELECT
    course_id,
    total_score,
    n_grade_status_withdrawn,
    enrollment_id,
    enrollment_score,
    enrollment_enrolled_at,
    enrollment_student_id,
    enrollment_term
FROM with_children_enrollments
WHERE enrollment_term = '2025T1'
