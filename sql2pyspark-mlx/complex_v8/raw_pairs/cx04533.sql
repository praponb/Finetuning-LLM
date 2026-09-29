WITH valid AS (
    -- Scope the enrollments rows
    SELECT *
    FROM enrollments
    WHERE course_id <= 7
      AND student_id >= 7
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        c.credits AS course_credits,
        c.dept AS course_dept,
        c.level AS course_level
    FROM valid s
    JOIN courses c ON c.id = s.course_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY course_level ORDER BY score DESC, id) AS score_seq,
        course_credits - LAG(course_credits) OVER w_grade_status_by_score AS course_credits_change
    FROM combined_details
    WINDOW w_grade_status_by_score AS (PARTITION BY grade_status ORDER BY score DESC, id)
),
line_level_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.course_credits,
        s.course_level,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id,
        e.score AS enrollment_score
    FROM windowed s
    JOIN enrollments e ON e.student_id = s.student_id
)
-- Subtotals with GROUPING SETS
SELECT
    enrollment_course_id,
    COALESCE(grade_status, 'ALL') AS grade_status,
    course_id,
    PERCENTILE_APPROX(enrollment_score, 0.5) AS median_enrollment_score,
    COUNT(*) FILTER (WHERE enrollment_term <> '2024T1') AS n_enrollment_term_2024t1,
    SUM(course_credits) AS total_course_credits
FROM line_level_enrollments
GROUP BY GROUPING SETS ((enrollment_course_id, grade_status, course_id), (enrollment_course_id), (grade_status), ())
ORDER BY enrollment_course_id NULLS LAST, grade_status NULLS LAST, course_id NULLS LAST
