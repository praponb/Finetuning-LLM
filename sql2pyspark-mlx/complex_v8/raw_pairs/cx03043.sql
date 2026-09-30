WITH cleaned_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 66.0 THEN 'large' WHEN score >= 34.2 THEN 'medium' ELSE 'small' END AS score_tier
    FROM enrollments
    WHERE grade_status IN ('FAIL', 'WITHDRAWN')
      AND score < 74.9
),
latest_per_key AS (
    -- Keep the latest row per course_id
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        score_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY enrolled_at DESC, id DESC) AS row_num
        FROM cleaned_enrollments
    ) ranked
    WHERE row_num = 1
)
SELECT
    grade_status,
    score_tier,
    SUM(score) FILTER (WHERE grade_status IN ('FAIL', 'INCOMPLETE')) AS score_grade_status_in_fail_incompl,
    MIN(score) AS min_score
FROM latest_per_key
GROUP BY ROLLUP(grade_status, score_tier)
ORDER BY grade_status NULLS LAST, score_tier NULLS LAST
