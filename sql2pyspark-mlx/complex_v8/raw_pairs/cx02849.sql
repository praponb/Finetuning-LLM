WITH prepared_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score BETWEEN 2.2 AND 66.0
),
enriched AS (
    -- Label and score rows
    SELECT *, DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year
    FROM prepared_enrollments
),
exploded_detail_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at_year,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.student_id AS enrollment_student_id
    FROM enriched s
    JOIN enrollments e ON e.course_id = s.course_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        enrollment_id,
        ROUND(100 * COUNT(CASE WHEN enrollment_grade_status IN ('FAIL', 'INCOMPLETE') THEN 1 END) / COUNT(*), 1) AS pct_enrollment_grade_status_in_f,
        PERCENTILE_APPROX(score, 0.5) AS median_score
    FROM exploded_detail_enrollments
    WHERE enrollment_grade_status = 'WITHDRAWN'
    GROUP BY enrollment_id
)
SELECT
    enrollment_id,
    pct_enrollment_grade_status_in_f,
    median_score,
    COALESCE(median_score, 0.0) AS median_score_filled,
    COALESCE(median_score, 0.0) AS median_score_filled_2
FROM rollup_base
WHERE median_score < 75.0
ORDER BY median_score_filled
