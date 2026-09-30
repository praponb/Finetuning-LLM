WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE course_id <= 9
      AND term NOT IN ('2025T2', '2025T1')
),
with_parent AS (
    -- Attach student attributes
    SELECT s.*, st.student_name, st.major AS student_major
    FROM valid s
    LEFT JOIN students st ON st.id = s.student_id AND NOT st.is_international
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(score - AVG(score) OVER w_course_id, 2) AS score_vs_course_id_avg,
        COUNT(*) OVER w_course_id AS rows_in_course_id,
        MIN(score) OVER (PARTITION BY course_id) AS course_id_min_score,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        SUM(score) OVER (PARTITION BY student_id, term ORDER BY score DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS score_sum_2,
        SIZE(COLLECT_SET(student_major) OVER (PARTITION BY course_id)) AS distinct_student_major_in_course_id,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY course_id)) AS distinct_grade_status_in_course_id,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY student_id, term ORDER BY score DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROUND(CUME_DIST() OVER (PARTITION BY term ORDER BY score DESC, id), 3) AS score_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY grade_status, TO_DATE(enrolled_at) ORDER BY score NULLS LAST, id) AS rn,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id
    FROM with_parent
    WINDOW w_course_id AS (PARTITION BY course_id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY distinct_student_major_in_course_id, grade_status) AS rows_in_distinct_student_major_in_course_id_grad,
        SUM(score_vs_course_id_avg) OVER w_distinct_student_major_in_co AS distinct_student_major_in_course_id_grade_status,
        MAX(score_vs_course_id_avg) OVER w_distinct_student_major_in_co AS distinct_student_major_in_course_id_grade_status_2,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY MONTH(enrolled_at), grade_status ORDER BY score_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        LEAD(rn, 3) OVER (PARTITION BY MONTH(enrolled_at), grade_status ORDER BY score_cume_dist DESC, id) AS next3_rn,
        ROW_NUMBER() OVER w_enrolled_at_month_grade_stat_2 AS seq_no,
        LAST_VALUE(score_sum_2) IGNORE NULLS OVER (PARTITION BY MONTH(enrolled_at), grade_status ORDER BY score_cume_dist DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score_sum_2
    FROM windowed
    WINDOW w_distinct_student_major_in_co AS (PARTITION BY distinct_student_major_in_course_id, grade_status), w_enrolled_at_month_grade_stat_2 AS (PARTITION BY MONTH(enrolled_at), grade_status ORDER BY score_cume_dist DESC, id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    enrolled_at,
    student_name,
    student_major,
    score_vs_course_id_avg,
    rows_in_course_id,
    course_id_min_score,
    score_sum_2,
    distinct_student_major_in_course_id,
    score_cume_dist,
    score_share_of_course_id,
    rows_in_distinct_student_major_in_course_id_grad,
    distinct_student_major_in_course_id_grade_status_2,
    last_enrolled_at,
    next3_rn,
    seq_no,
    last_known_score_sum_2
FROM ranked
ORDER BY distinct_student_major_in_course_id_grade_status_2 DESC
