WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE (grade_status = 'INCOMPLETE' OR course_id IS NOT NULL)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        term_len,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY MONTH(enrolled_at), course_id)) AS distinct_grade_status_in_enrolled_at_month_cours,
        ROW_NUMBER() OVER (PARTITION BY course_id, student_id ORDER BY term_len DESC, id) AS seq_no,
        MIN(term_len) OVER w_enrolled_at_month_course_id AS enrolled_at_month_course_id_min_term_len,
        LAG(term_len) OVER w_course_id_student_id_by_term AS prev_term_len,
        ROUND(term_len / NULLIF(SUM(term_len) OVER (PARTITION BY MONTH(enrolled_at), course_id), 0), 4) AS term_len_share_of_enrolled_at_month_course_id
    FROM cleaned
    WINDOW w_enrolled_at_month_course_id AS (PARTITION BY MONTH(enrolled_at), course_id), w_course_id_student_id_by_term AS (PARTITION BY course_id, student_id ORDER BY term_len DESC, id)
)
SELECT
    distinct_grade_status_in_enrolled_at_month_cours,
    prev_term_len,
    COUNT(*) AS cnt,
    ROUND(100 * COUNT(CASE WHEN grade_status IN ('PASS', 'WITHDRAWN', 'INCOMPLETE') THEN 1 END) / COUNT(*), 1) AS pct_grade_status_in_pass_withdra
FROM with_trends
GROUP BY CUBE(distinct_grade_status_in_enrolled_at_month_cours, prev_term_len)
ORDER BY distinct_grade_status_in_enrolled_at_month_cours NULLS LAST, prev_term_len NULLS LAST
