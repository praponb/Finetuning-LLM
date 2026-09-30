WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE grade_status IS NOT NULL
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.course_id = s.course_id) AS max_enrollment_score,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS last_enrollment_enrolled_at
    FROM recent_enrollments s
),
totals AS (
    -- Group-level totals
    SELECT
        CASE WHEN score >= 62.5 THEN 'large' WHEN score >= 32.7 THEN 'medium' ELSE 'small' END AS score_tier,
        grade_status,
        SUM(CASE WHEN grade_status NOT IN ('WITHDRAWN', 'INCOMPLETE') THEN max_enrollment_score ELSE 0 END) AS max_enrollment_score_grade_status_not_in_withdra,
        COUNT(*) FILTER (WHERE grade_status = 'FAIL') AS n_grade_status_fail,
        SUM(max_enrollment_score) FILTER (WHERE grade_status = 'FAIL') AS max_enrollment_score_grade_status_fail,
        ROUND(STDDEV(enrollment_count), 2) AS stddev_enrollment_count,
        COUNT(DISTINCT grade_status) AS distinct_grade_status
    FROM summarized_rows
    WHERE course_id IS NOT NULL
    GROUP BY CASE WHEN score >= 62.5 THEN 'large' WHEN score >= 32.7 THEN 'medium' ELSE 'small' END, grade_status
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM totals
    WHERE distinct_grade_status > (SELECT MIN(distinct_grade_status) FROM totals)
)
SELECT *
FROM (
    SELECT n_grade_status_fail, distinct_grade_status, score_tier, stddev_enrollment_count
    FROM screened
) src
PIVOT (
    MIN(stddev_enrollment_count) AS min_v, SUM(stddev_enrollment_count) AS sum_v
    FOR score_tier IN ('medium', 'small', 'large')
)
ORDER BY n_grade_status_fail, distinct_grade_status
