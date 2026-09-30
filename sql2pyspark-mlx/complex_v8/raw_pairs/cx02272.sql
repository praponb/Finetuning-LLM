WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref,
        SUBSTRING(TRIM(grade_status), 1, 2) AS grade_status_prefix
    FROM enrollments
    WHERE term NOT IN ('2024T2', '2025T1')
      AND TO_DATE(enrolled_at) < DATE '2025-02-12'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(score) OVER (PARTITION BY grade_status, course_id) AS grade_status_course_id_min_score,
        SIZE(COLLECT_SET(term) OVER w_course_id_student_id) AS distinct_term_in_course_id_student_id,
        ROUND(AVG(score) OVER w_grade_status_course_id, 2) AS grade_status_course_id_avg_score,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY student_id, term ORDER BY enrolled_at ASC NULLS LAST, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        NTILE(4) OVER (ORDER BY enrolled_at DESC, id) AS enrolled_at_quartile,
        MAX(score) OVER (PARTITION BY course_id, student_id) AS course_id_student_id_max_score,
        NTILE(4) OVER (PARTITION BY grade_status ORDER BY score DESC, id) AS score_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY grade_status ORDER BY enrolled_at ASC), 3) AS enrolled_at_percent_rank
    FROM prepared
    WINDOW w_course_id_student_id AS (PARTITION BY course_id, student_id), w_grade_status_course_id AS (PARTITION BY grade_status, course_id)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(enrolled_at_quartile / NULLIF(score_quartile, 0), 3) AS enrolled_at_quartile_per_score_quartile
    FROM windowed
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(enrolled_at) AS enrolled_at_year,
        enrolled_at_quartile,
        course_id,
        COUNT(DISTINCT term) AS distinct_terms,
        PERCENTILE_APPROX(distinct_term_in_course_id_student_id, 0.5) AS median_distinct_term_in_course_id_student_id,
        COUNT(*) FILTER (WHERE term = '2024T1') AS n_term_2024t1,
        MAX(score_quartile) AS max_score_quartile,
        COUNT(DISTINCT grade_status) AS distinct_grade_status
    FROM labeled
    GROUP BY YEAR(enrolled_at), enrolled_at_quartile, course_id
)
SELECT
    enrolled_at_year,
    enrolled_at_quartile,
    course_id,
    distinct_terms,
    median_distinct_term_in_course_id_student_id,
    n_term_2024t1,
    max_score_quartile,
    ABS(distinct_terms - 6) AS distinct_terms_distance
FROM totals
ORDER BY enrolled_at_year DESC, enrolled_at_quartile, course_id
LIMIT 20
