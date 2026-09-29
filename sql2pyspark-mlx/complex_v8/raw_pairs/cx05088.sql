WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE term = '2024T2'
),
with_details AS (
    -- Join students
    SELECT s.*, st.major AS student_major, st.enrolled_on AS student_enrolled_on
    FROM filtered_enrollments s
    LEFT JOIN students st ON st.id = s.student_id AND st.cohort IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        score,
        grade_status,
        enrolled_at,
        student_enrolled_on,
        ROUND(CUME_DIST() OVER w_all_by_enrolled_at_ties, 3) AS enrolled_at_cume_dist,
        NTH_VALUE(score, 2) OVER (PARTITION BY course_id, student_major ORDER BY student_enrolled_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        NTH_VALUE(score, 2) OVER (PARTITION BY grade_status, student_id ORDER BY student_enrolled_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_2,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_grade_status_student_id_by_s_2)) / 60, 1) AS minutes_since_prev_enrolled_at,
        MAX(score) OVER (PARTITION BY student_major, YEAR(student_enrolled_on) ORDER BY score, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_max_3,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        LAST_VALUE(enrolled_at) OVER (ORDER BY student_enrolled_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id), 0), 4) AS score_share_of_student_id,
        SUM(score) OVER (ORDER BY enrolled_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS score_sum_4,
        ROW_NUMBER() OVER (PARTITION BY student_major, YEAR(student_enrolled_on) ORDER BY score, id) AS seq_no
    FROM with_details
    WINDOW w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at), w_grade_status_student_id_by_s_2 AS (PARTITION BY grade_status, student_id ORDER BY student_enrolled_on DESC, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        score,
        grade_status,
        enrolled_at,
        student_enrolled_on,
        enrolled_at_cume_dist,
        second_score,
        second_score_2,
        minutes_since_prev_enrolled_at,
        score_max_3,
        first_known_enrolled_at,
        last_enrolled_at,
        score_share_of_student_id,
        score_sum_4
    FROM sequenced
    WHERE seq_no = 1
)
SELECT
    id,
    score,
    grade_status,
    enrolled_at,
    student_enrolled_on,
    enrolled_at_cume_dist,
    second_score,
    second_score_2,
    minutes_since_prev_enrolled_at,
    score_max_3,
    first_known_enrolled_at,
    score_share_of_student_id,
    score_sum_4,
    score_max_3 - second_score_2 AS score_max_3_minus_second_score_2,
    ROUND(enrolled_at_cume_dist * 1.1, 2) AS enrolled_at_cume_dist_adj
FROM top_rows
WHERE grade_status = 'PASS'
ORDER BY enrolled_at_cume_dist
