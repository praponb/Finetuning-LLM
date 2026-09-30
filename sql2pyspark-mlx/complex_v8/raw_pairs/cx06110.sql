WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        QUARTER(enrolled_at) AS enrolled_at_quarter,
        CASE WHEN score >= 56.1 THEN 'A' WHEN score >= 35.8 THEN 'B' ELSE 'C' END AS score_tier
    FROM enrollments
    WHERE student_id <= 12
),
enriched_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at_quarter,
        c.level AS course_level,
        c.dept AS course_dept
    FROM base s
    LEFT JOIN courses c ON c.id = s.course_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(5) OVER w_course_dept_by_score AS score_quintile,
        score - LAG(score) OVER w_course_level_by_score AS score_change,
        LAST_VALUE(enrolled_at_quarter) OVER (PARTITION BY course_dept ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at_quarter,
        NTH_VALUE(enrolled_at_quarter, 2) OVER (PARTITION BY student_id ORDER BY score DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_quarter,
        ROUND(PERCENT_RANK() OVER w_student_id_by_score_ties, 3) AS score_percent_rank,
        ROUND(score / NULLIF(SUM(score) OVER w_student_id_course_dept, 0), 4) AS score_share_of_student_id_course_dept,
        SUM(score) OVER (PARTITION BY student_id, course_dept) AS student_id_course_dept_sum_score,
        LEAD(score) OVER (PARTITION BY course_level ORDER BY score, id) AS next_score,
        score - LAG(score) OVER (PARTITION BY course_dept ORDER BY score DESC, id) AS score_change_2,
        ROUND(enrolled_at_quarter - AVG(enrolled_at_quarter) OVER (PARTITION BY student_id, course_dept), 2) AS enrolled_at_quarter_vs_student_id_course_dept_av,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id), 0), 4) AS score_share_of_student_id,
        COUNT(*) OVER w_student_id_course_dept AS rows_in_student_id_course_dept
    FROM enriched_course
    WINDOW w_course_dept_by_score AS (PARTITION BY course_dept ORDER BY score DESC, id), w_course_level_by_score AS (PARTITION BY course_level ORDER BY score, id), w_student_id_by_score_ties AS (PARTITION BY student_id ORDER BY score DESC NULLS FIRST), w_student_id_course_dept AS (PARTITION BY student_id, course_dept)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    enrolled_at_quarter,
    score_change,
    last_enrolled_at_quarter,
    second_enrolled_at_quarter,
    score_percent_rank,
    next_score,
    score_change_2,
    enrolled_at_quarter_vs_student_id_course_dept_av,
    score_share_of_student_id,
    rows_in_student_id_course_dept
FROM with_windows
ORDER BY term DESC
