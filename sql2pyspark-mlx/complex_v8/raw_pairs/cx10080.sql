WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        ROUND(score * 0.9, 2) AS score_adj,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix
    FROM enrollments
    WHERE score < 83.1
      AND course_id <= 9
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.grade_status_prefix,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.score AS enrollment_score
    FROM valid_enrollments s
    JOIN enrollments e ON e.course_id = s.course_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(score) OVER (ORDER BY score NULLS LAST, id, enrollment_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS score_avg_6_prior,
        RANK() OVER (ORDER BY score NULLS LAST) AS score_rank
    FROM line_level
),
without_matches AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM with_trends s
    WHERE EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND LENGTH(TRIM(st.student_name)) >= 8
              AND st.cohort = 'C2022'
        )
)
SELECT
    student_id,
    term,
    COUNT(DISTINCT course_id) AS distinct_course_ids,
    COUNT(DISTINCT student_id) AS distinct_student_ids,
    ARRAY_SORT(COLLECT_SET(term)) AS term_list
FROM without_matches
GROUP BY GROUPING SETS ((student_id, term), (student_id))
