WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM enrollments
    WHERE term = '2024T2'
      AND HOUR(enrolled_at) BETWEEN 10 AND 16
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(score_bucket) OVER (PARTITION BY student_id, course_id) AS student_id_course_id_min_score_bucket,
        ROW_NUMBER() OVER w_all_by_score AS seq_no,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        MIN(score_bucket) OVER w_enrolled_at_month_term AS enrolled_at_month_term_min_score_bucket,
        ROUND(CUME_DIST() OVER (ORDER BY score_bucket, id), 3) AS score_bucket_cume_dist,
        ROUND(score / NULLIF(SUM(score) OVER w_student_id_course_id, 0), 4) AS score_share_of_student_id_course_id,
        ROUND(score_bucket / NULLIF(SUM(score_bucket) OVER (PARTITION BY MONTH(enrolled_at), term), 0), 4) AS score_bucket_share_of_enrolled_at_month_term,
        LAG(score, 2) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS prev2_score,
        SIZE(COLLECT_SET(grade_status) OVER w_student_id_course_id) AS distinct_grade_status_in_student_id_course_id,
        LAG(score, 3) OVER w_student_id_by_enrolled_at AS prev3_score
    FROM filtered_enrollments
    WINDOW w_all_by_score AS (ORDER BY score, id), w_enrolled_at_month_term AS (PARTITION BY MONTH(enrolled_at), term), w_student_id_course_id AS (PARTITION BY student_id, course_id), w_student_id_by_enrolled_at AS (PARTITION BY student_id ORDER BY enrolled_at, id)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE seq_no = 1
),
stats AS (
    -- Summarise per group
    SELECT
        grade_status,
        term,
        distinct_grade_status_in_student_id_course_id,
        ROUND(SUM(score_share_of_student_id_course_id), 2) AS total_score_share_of_student_id_course_id,
        SUM(score_bucket) AS total_score_bucket
    FROM leaders
    GROUP BY grade_status, term, distinct_grade_status_in_student_id_course_id
    HAVING MAX(score_share_of_student_id_course_id) > 0.3
)
-- Final projection
SELECT
    grade_status,
    term,
    distinct_grade_status_in_student_id_course_id,
    total_score_share_of_student_id_course_id,
    SUBSTRING(TRIM(term), 1, 2) AS term_prefix
FROM stats
ORDER BY term_prefix NULLS LAST, grade_status, term, distinct_grade_status_in_student_id_course_id
LIMIT 5
