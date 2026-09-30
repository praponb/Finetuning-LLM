WITH base AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE student_id >= 12
      AND course_id <= 11
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY enrolled_at DESC) AS enrolled_at_rank,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id, grade_status), 0), 4) AS score_share_of_student_id_grade_status
    FROM base
)
SELECT
    course_id,
    student_id,
    COALESCE(grade_status, 'ALL') AS grade_status,
    MIN(score) AS min_score,
    ROUND(100 * COUNT(CASE WHEN grade_status = 'FAIL' THEN 1 END) / COUNT(*), 1) AS pct_grade_status_fail,
    ROUND(100 * COUNT(CASE WHEN grade_status IN ('WITHDRAWN', 'FAIL', 'PASS') THEN 1 END) / COUNT(*), 1) AS pct_grade_status_in_withdrawn_fa
FROM windowed
GROUP BY GROUPING SETS ((course_id, student_id, grade_status), (course_id), ())
ORDER BY course_id NULLS LAST, student_id NULLS LAST, grade_status NULLS LAST
