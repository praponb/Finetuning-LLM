WITH prepared_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        CONCAT_WS(': ', grade_status, term) AS grade_status_term_label,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        INITCAP(TRIM(term)) AS term_clean
    FROM enrollments
    WHERE (term NOT IN ('2025T2', '2024T2') OR MONTH(enrolled_at) IN (2, 3))
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        enrolled_at,
        ROUND(grade_status_len / NULLIF(SUM(grade_status_len) OVER w_student_id_term, 0), 4) AS grade_status_len_share_of_student_id_term,
        FIRST_VALUE(score) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        SUM(grade_status_len) OVER w_student_id_term AS student_id_term_sum_grade_status_len,
        COUNT(*) OVER w_student_id_term AS rows_in_student_id_term,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id, term), 0), 4) AS score_share_of_student_id_term,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at
    FROM prepared_enrollments
    WINDOW w_student_id_term AS (PARTITION BY student_id, term)
),
enriched AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status_len_share_of_student_id_term,
        s.first_score,
        s.student_id_term_sum_grade_status_len,
        s.score_share_of_student_id_term,
        s.second_enrolled_at,
        c.level AS course_level
    FROM with_trends s
    JOIN courses c ON c.id = s.course_id AND c.level IN ('advanced', 'intro')
    WHERE c.level NOT IN ('core', 'advanced')
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status_len_share_of_student_id_term,
    student_id_term_sum_grade_status_len,
    score_share_of_student_id_term,
    course_level
FROM enriched
WHERE course_level <> 'intro'
ORDER BY score_share_of_student_id_term DESC
