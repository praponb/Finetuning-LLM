WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 0.9, 2) AS score_adj,
        ROUND(score * 1.1, 2) AS score_adj_2,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE score > 8.7
),
grouped AS (
    -- Group-level totals
    SELECT
        grade_status,
        student_id,
        course_id,
        PERCENTILE_APPROX(score_adj, 0.5) AS median_score_adj,
        MAX(enrolled_at) AS last_enrolled_at
    FROM recent
    GROUP BY grade_status, student_id, course_id
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.grade_status,
        s.student_id,
        s.course_id,
        s.last_enrolled_at,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status,
        e.course_id AS enrollment_course_id
    FROM grouped s
    JOIN enrollments e ON e.student_id = s.student_id
),
filtered AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM with_children_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.enrollment_course_id
              AND e.enrolled_at >= TIMESTAMP '2025-01-31 15:00:00'
        )
),
joined_course AS (
    -- Join courses
    SELECT
        s.grade_status,
        s.student_id,
        s.course_id,
        s.last_enrolled_at,
        s.enrollment_id,
        s.enrollment_score,
        s.enrollment_grade_status,
        s.enrollment_course_id,
        c.credits AS course_credits,
        c.course_code,
        e.score AS enrollment_score_2
    FROM filtered s
    JOIN courses c ON c.id = s.course_id
    JOIN enrollments e ON e.id = s.enrollment_id
),
with_parent_enrollment AS (
    -- Attach enrollment attributes
    SELECT
        s.*,
        e.student_id AS enrollment_student_id,
        e.score AS enrollment_score_3,
        e.term AS enrollment_term
    FROM joined_course s
    JOIN enrollments e ON e.id = s.enrollment_id
),
qualified AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM with_parent_enrollment s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.enrolled_at >= TIMESTAMP '2025-01-25 13:00:00'
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT grade_status, student_id, metric_name, amount
FROM (
    SELECT grade_status, student_id, enrollment_score_2, enrollment_score
    FROM qualified
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric_name IN (enrollment_score_2 AS enrollment_score_2_v, enrollment_score AS enrollment_score_v)
) unp
ORDER BY grade_status, student_id, metric_name
