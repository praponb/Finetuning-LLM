WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-19 15:00:00'
      AND student_id >= 8
),
with_details_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        st.is_international AS student_is_international
    FROM scoped_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM with_details_student
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.student_id = s.student_id) AS avg_enrollment_score,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS last_enrollment_enrolled_at
    FROM enriched s
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.student_is_international,
        s.grade_status_or_unknown,
        s.avg_enrollment_score,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at
    FROM with_stats s
    JOIN enrollments e ON e.student_id = s.student_id AND e.score >= 17.4
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        grade_status_or_unknown,
        enrollment_id,
        enrollment_term,
        enrollment_enrolled_at,
        LEAD(enrollment_enrolled_at) OVER (PARTITION BY enrollment_id, grade_status_or_unknown ORDER BY avg_enrollment_score DESC, id, enrollment_id) AS next_enrollment_enrolled_at,
        MIN(avg_enrollment_score) OVER (ORDER BY enrollment_score, id, enrollment_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_avg_enrollment_score_so_far,
        COUNT(*) OVER (PARTITION BY enrollment_id) AS rows_in_enrollment_id,
        ROUND(AVG(avg_enrollment_score) OVER (PARTITION BY enrollment_id, grade_status_or_unknown ORDER BY avg_enrollment_score DESC, id, enrollment_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS avg_enrollment_score_avg_7,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY enrollment_id)) AS distinct_course_id_in_enrollment_id
    FROM with_children
),
with_parent_student AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        st.gpa AS student_gpa,
        st.is_international AS student_is_international
    FROM sequenced s
    JOIN students st ON st.id = s.student_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrollment_term, student_id, grade_status_or_unknown, avg_enrollment_score_avg_7
    FROM with_parent_student
) src
PIVOT (
    COUNT(avg_enrollment_score_avg_7)
    FOR grade_status_or_unknown IN ('WITHDRAWN', 'INCOMPLETE')
)
