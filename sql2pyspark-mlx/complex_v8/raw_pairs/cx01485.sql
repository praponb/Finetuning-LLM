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
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
    WHERE student_id IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(enrolled_at, 3) OVER (PARTITION BY term_or_unknown, student_id ORDER BY enrolled_at_date, id) AS prev3_enrolled_at,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        RANK() OVER w_term_or_unknown_student_id_b AS enrolled_at_date_rank,
        ROUND(score - AVG(score) OVER w_course_id, 2) AS score_vs_course_id_avg
    FROM filtered_enrollments
    WINDOW w_term_or_unknown_student_id_b AS (PARTITION BY term_or_unknown, student_id ORDER BY enrolled_at_date, id), w_course_id AS (PARTITION BY course_id)
)
SELECT
    student_id,
    enrolled_at_date_rank,
    course_id,
    COUNT(*) AS row_count,
    COUNT(DISTINCT grade_status) AS distinct_grade_status,
    ROUND(SUM(score_vs_course_id_avg) / NULLIF(SUM(enrolled_at_date_rank), 0), 3) AS score_vs_course_id_avg_to_enrolled_at_date_rank_
FROM ranked
GROUP BY CUBE(student_id, enrolled_at_date_rank, course_id)
ORDER BY student_id NULLS LAST, enrolled_at_date_rank NULLS LAST, course_id NULLS LAST
