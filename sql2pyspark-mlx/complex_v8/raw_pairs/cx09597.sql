WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.07, 2) AS score_adj,
        CONCAT(term, '#', CAST(id AS STRING)) AS term_ref
    FROM enrollments
    WHERE (term IN ('2025T2', '2025T1', '2024T1') OR grade_status = 'WITHDRAWN')
),
with_parent AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.score_adj,
        s.term_ref,
        c.dept AS course_dept,
        c.credits AS course_credits,
        c.level AS course_level
    FROM cleaned_enrollments s
    JOIN courses c ON c.id = s.course_id AND c.level IN ('advanced', 'intro')
    WHERE c.dept = 'Biz'
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS max_enrollment_score
    FROM with_parent s
),
latest_per_key AS (
    -- Keep the earliest row per course_dept, course_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        score_adj,
        term_ref,
        course_dept,
        course_credits,
        course_level,
        max_enrollment_score
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_dept, course_id ORDER BY enrolled_at, id) AS rn
        FROM summarized_rows
    ) ranked
    WHERE rn = 1
)
SELECT
    term,
    student_id,
    COUNT(DISTINCT student_id) AS distinct_student_ids,
    COUNT(*) FILTER (WHERE grade_status IN ('PASS', 'FAIL')) AS n_grade_status_in_pass_fail,
    COUNT(DISTINCT term) AS distinct_terms
FROM latest_per_key
GROUP BY CUBE(term, student_id)
