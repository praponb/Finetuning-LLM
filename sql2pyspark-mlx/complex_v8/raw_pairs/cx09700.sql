WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE term = '2025T2'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(AVG(score) OVER (PARTITION BY student_id, grade_status), 2) AS student_id_grade_status_avg_score,
        ROUND(AVG(score) OVER (PARTITION BY student_id ORDER BY score, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS score_avg_1_prior,
        ROUND(AVG(score) OVER (PARTITION BY grade_status, student_id ORDER BY score DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7
    FROM base
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('DAY', enrolled_at) AS enrolled_at_day,
        FLOOR(score / 25) * 25 AS score_bucket,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM with_trends
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    student_id_grade_status_avg_score,
    score_avg_1_prior,
    enrolled_at_day,
    score_bucket,
    grade_status_len
FROM tagged
WHERE TO_DATE(enrolled_at_day) < DATE '2025-02-09'
ORDER BY score DESC
