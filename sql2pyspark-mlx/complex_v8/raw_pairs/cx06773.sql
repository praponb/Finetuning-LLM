WITH cleaned_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 38.5) AS score_distance,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM cleaned_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND MONTH(e.enrolled_at) IN (1, 2)
              AND e.term NOT IN ('2025T1', '2025T2')
        )
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS min_enrollment_score,
        (SELECT COUNT(*) FILTER (WHERE e.grade_status <> 'FAIL') FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_grade_status_count,
        (SELECT COUNT(CASE WHEN e.term IN ('2024T2', '2025T1') THEN 1 END) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_term_count
    FROM screened s
),
with_details_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.min_enrollment_score,
        s.enrollment_grade_status_count,
        s.enrollment_term_count,
        c.level AS course_level,
        c.credits AS course_credits,
        c.course_code
    FROM with_counts s
    JOIN courses c ON c.id = s.course_id
),
grade_status_stats AS (
    -- Avg min_enrollment_score per grade_status
    SELECT grade_status, AVG(min_enrollment_score) AS avg_min_enrollment_score
    FROM with_details_course
    GROUP BY grade_status
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_min_enrollment_score,
        ROUND(s.min_enrollment_score - g.avg_min_enrollment_score, 2) AS min_enrollment_score_minus_avg
    FROM with_details_course s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.min_enrollment_score > g.avg_min_enrollment_score
),
screened_2 AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM vs_group s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.student_id <= 8
        )
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY score, id) AS rn,
        LEAD(min_enrollment_score) OVER w_all_by_score AS next_min_enrollment_score,
        LAG(enrolled_at) OVER w_all_by_score AS prev_enrolled_at,
        COUNT(*) OVER (PARTITION BY TO_DATE(enrolled_at)) AS rows_in_enrolled_at_to_date
    FROM screened_2
    WINDOW w_all_by_score AS (ORDER BY score, id)
)
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    min_enrollment_score,
    enrollment_grade_status_count,
    enrollment_term_count,
    course_level,
    course_code,
    rn,
    next_min_enrollment_score,
    rows_in_enrolled_at_to_date
FROM sequenced
ORDER BY score, grade_status
