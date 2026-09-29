WITH filtered AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 2) AS grade_status_prefix,
        CASE WHEN score >= 80.2 THEN 'A' WHEN score >= 11.3 THEN 'B' ELSE 'C' END AS score_tier
    FROM enrollments
    WHERE student_id <= 17
      AND term IN ('2025T1', '2024T1', '2025T2')
),
joined AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.score_tier,
        c.level AS course_level,
        c.course_code
    FROM filtered s
    LEFT JOIN courses c ON c.id = s.course_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS first_enrollment_enrolled_at,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.course_id = s.course_id) AS max_enrollment_score,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS last_enrollment_enrolled_at
    FROM joined s
)
SELECT
    grade_status,
    score_tier,
    COUNT(DISTINCT grade_status) AS distinct_grade_status
FROM summarized_rows
GROUP BY CUBE(grade_status, score_tier)
ORDER BY grade_status NULLS LAST, score_tier NULLS LAST
