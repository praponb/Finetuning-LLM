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
        ROUND(score * 1.5, 2) AS score_adj,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
),
course_id_stats AS (
    -- Median score per course_id
    SELECT course_id, PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM cleaned_enrollments
    GROUP BY course_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_score,
        ROUND(s.score - g.median_score, 2) AS score_minus_median
    FROM cleaned_enrollments s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.score >= g.median_score
),
student_id_stats AS (
    -- Max median_score per student_id
    SELECT student_id, MAX(median_score) AS max_median_score
    FROM outliers
    GROUP BY student_id
),
outliers_2 AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_median_score,
        ROUND(s.median_score - g.max_median_score, 2) AS median_score_minus_max
    FROM outliers s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.median_score = g.max_median_score
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        MAX(score) AS max_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at,
        e.max_enrollment_score
    FROM outliers_2 s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
)
SELECT *
FROM (
    SELECT course_id, last_enrollment_enrolled_at, grade_status_or_unknown, score_adj
    FROM summarized_rows
) src
PIVOT (
    AVG(score_adj)
    FOR grade_status_or_unknown IN ('PASS', 'INCOMPLETE', 'FAIL')
)
ORDER BY course_id, last_enrollment_enrolled_at
