WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        FLOOR(score / 25) * 25 AS score_bucket,
        SUBSTRING(TRIM(term), 1, 3) AS term_prefix
    FROM enrollments
    WHERE course_id IS NOT NULL
),
student_id_stats AS (
    -- Avg score per student_id
    SELECT student_id, AVG(score) AS avg_score
    FROM scoped_enrollments
    GROUP BY student_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM scoped_enrollments s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.score > g.avg_score
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(score - AVG(score) OVER (PARTITION BY course_id, grade_status), 2) AS score_vs_course_id_grade_status_avg,
        ROUND(AVG(score_bucket) OVER (PARTITION BY term, student_id ORDER BY score_bucket DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_bucket_avg_7,
        RANK() OVER (PARTITION BY grade_status, student_id ORDER BY avg_score DESC, id) AS avg_score_rank,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY course_id, grade_status)) AS distinct_term_in_course_id_grade_status
    FROM vs_group
)
SELECT
    id,
    course_id,
    term,
    score,
    score_bucket,
    term_prefix,
    avg_score,
    score_minus_avg,
    COALESCE(score_bucket, 0) AS score_bucket_filled,
    LENGTH(TRIM(term)) AS term_len
FROM with_windows
ORDER BY id DESC
