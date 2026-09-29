WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('INCOMPLETE', 'PASS') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE score >= 20.2
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        grade_status_len,
        score_filled,
        COALESCE(grade_status_len, 0) AS grade_status_len_filled
    FROM recent_enrollments
),
grade_status_stats AS (
    -- Max grade_status_len per grade_status
    SELECT grade_status, MAX(grade_status_len) AS max_grade_status_len
    FROM scored
    GROUP BY grade_status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_grade_status_len,
        ROUND(s.grade_status_len - g.max_grade_status_len, 2) AS grade_status_len_minus_max
    FROM scored s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.grade_status_len = g.max_grade_status_len
)
SELECT
    grade_status_len,
    COALESCE(grade_status, 'ALL') AS grade_status,
    GROUPING(grade_status_len) AS is_total_grade_status_len,
    ROUND(SUM(grade_status_len_minus_max) / NULLIF(SUM(grade_status_len_filled), 0), 3) AS grade_status_len_minus_max_to_grade_status_len_f,
    COUNT(DISTINCT term) AS distinct_terms
FROM outliers
GROUP BY CUBE(grade_status_len, grade_status)
ORDER BY grade_status_len NULLS LAST, grade_status NULLS LAST
