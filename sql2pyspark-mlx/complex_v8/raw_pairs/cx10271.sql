WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('QUARTER', enrolled_at) AS enrolled_at_quarter
    FROM enrollments
    WHERE enrolled_at < TIMESTAMP '2025-02-24 08:00:00'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(score, 2) OVER (PARTITION BY grade_status ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        MAX(score) OVER (PARTITION BY grade_status ORDER BY enrolled_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS score_max_6_prior,
        NTH_VALUE(enrolled_at_quarter, 2) OVER (PARTITION BY grade_status ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_quarter,
        ROUND(score / NULLIF(SUM(score) OVER w_term_student_id, 0), 4) AS score_share_of_term_student_id,
        LAG(score, 2, 0) OVER (PARTITION BY grade_status ORDER BY score DESC, id) AS prev2_score,
        FIRST_VALUE(enrolled_at_quarter) OVER (PARTITION BY grade_status ORDER BY enrolled_at_quarter DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at_quarter,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at_quarter)), 0), 4) AS score_share_of_enrolled_at_quarter_month,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id, grade_status), 0), 4) AS score_share_of_student_id_grade_status,
        SIZE(COLLECT_SET(course_id) OVER w_enrolled_at_quarter_month) AS distinct_course_id_in_enrolled_at_quarter_month,
        ROUND(score - AVG(score) OVER w_student_id_grade_status, 2) AS score_vs_student_id_grade_status_avg,
        MAX(score) OVER (PARTITION BY grade_status ORDER BY enrolled_at_quarter DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_so_far
    FROM scoped_enrollments
    WINDOW w_term_student_id AS (PARTITION BY term, student_id), w_enrolled_at_quarter_month AS (PARTITION BY DATE_TRUNC('MONTH', enrolled_at_quarter)), w_student_id_grade_status AS (PARTITION BY student_id, grade_status)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY course_id, grade_status ORDER BY score_vs_student_id_grade_status_avg DESC) AS score_vs_student_id_grade_status_avg_rank,
        NTILE(3) OVER (PARTITION BY course_id, grade_status ORDER BY score_vs_student_id_grade_status_avg DESC, id) AS score_vs_student_id_grade_status_avg_tercile,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY course_id, grade_status ORDER BY score_vs_student_id_grade_status_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        MAX(score_max_6_prior) OVER (PARTITION BY student_id) AS student_id_max_score_max_6_prior
    FROM ranked
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at_quarter,
    second_score,
    score_max_6_prior,
    second_enrolled_at_quarter,
    score_share_of_term_student_id,
    prev2_score,
    score_share_of_student_id_grade_status,
    distinct_course_id_in_enrolled_at_quarter_month,
    score_vs_student_id_grade_status_avg,
    max_score_so_far,
    score_vs_student_id_grade_status_avg_rank,
    score_vs_student_id_grade_status_avg_tercile,
    student_id_max_score_max_6_prior,
    DATE_TRUNC('HOUR', enrolled_at_quarter) AS enrolled_at_quarter_hour,
    CASE WHEN grade_status IN ('FAIL', 'INCOMPLETE') THEN 'group_a' WHEN grade_status = 'PASS' THEN 'group_b' ELSE 'other' END AS grade_status_group
FROM with_windows
WHERE score_vs_student_id_grade_status_avg_tercile < 9
