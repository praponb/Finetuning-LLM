WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        ABS(score - 32.9) AS score_distance,
        HOUR(enrolled_at) AS enrolled_at_hour
    FROM enrollments
    WHERE course_id IS NOT NULL
      AND HOUR(enrolled_at) BETWEEN 8 AND 12
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM recent s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
deduped AS (
    -- Keep the latest row per student_id, course_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        score_distance,
        enrolled_at_hour,
        enrollment_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY student_id, course_id ORDER BY score_distance DESC, id DESC) AS row_num
        FROM with_counts
    ) ranked
    WHERE row_num = 1
),
combined_details_course AS (
    -- Join courses
    SELECT
        s.*,
        c.dept AS course_dept,
        c.course_code,
        c.credits AS course_credits
    FROM deduped s
    JOIN courses c ON c.id = s.course_id
),
with_parent_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.enrollment_count,
        s.course_dept,
        s.course_code,
        c.course_code AS course_course_code,
        c.level AS course_level
    FROM combined_details_course s
    LEFT JOIN courses c ON c.id = s.course_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY term ORDER BY score DESC), 3) AS score_cume_dist,
        SUM(enrollment_count) OVER (PARTITION BY term, student_id) AS term_student_id_sum_enrollment_count,
        COUNT(*) OVER (PARTITION BY term, student_id) AS rows_in_term_student_id,
        ROUND(enrollment_count / NULLIF(SUM(enrollment_count) OVER (PARTITION BY term, student_id), 0), 4) AS enrollment_count_share_of_term_student_id,
        ROUND(enrollment_count - AVG(enrollment_count) OVER w_term_student_id, 2) AS enrollment_count_vs_term_student_id_avg,
        ROUND(score - AVG(score) OVER w_term_student_id, 2) AS score_vs_term_student_id_avg
    FROM with_parent_course
    WINDOW w_term_student_id AS (PARTITION BY term, student_id)
)
SELECT
    id,
    term,
    score,
    enrolled_at,
    course_dept,
    course_code,
    course_course_code,
    score_cume_dist,
    enrollment_count_share_of_term_student_id,
    score_vs_term_student_id_avg,
    CONCAT(course_dept, '#', CAST(id AS STRING)) AS course_dept_ref
FROM with_trends
WHERE LOWER(course_code) LIKE '%10%'
ORDER BY score_vs_term_student_id_avg NULLS LAST, id
LIMIT 20
