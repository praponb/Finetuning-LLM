WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE (student_id IS NOT NULL OR term <> '2025T2')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        LAG(score, 2) OVER (PARTITION BY student_id ORDER BY score DESC, id) AS prev2_score,
        RANK() OVER w_all_by_enrolled_at_ties AS enrolled_at_rank,
        ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY score DESC, id) AS seq_no,
        LAG(enrolled_at) OVER w_enrolled_at_to_date_student AS prev_enrolled_at,
        LEAD(enrolled_at) OVER (PARTITION BY student_id ORDER BY score DESC, id) AS next_enrolled_at,
        NTH_VALUE(score, 2) OVER (PARTITION BY student_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        SUM(score) OVER (PARTITION BY student_id, course_id ORDER BY enrolled_at ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_sum_2_prior,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY YEAR(enrolled_at), term), 0), 4) AS score_share_of_enrolled_at_year_term,
        LEAD(score) OVER (PARTITION BY student_id ORDER BY score DESC, id) AS next_score,
        MAX(score) OVER (PARTITION BY student_id) AS student_id_max_score,
        SIZE(COLLECT_SET(course_id) OVER w_enrolled_at_year_term) AS distinct_course_id_in_enrolled_at_year_term,
        ROUND(AVG(score) OVER (PARTITION BY TO_DATE(enrolled_at), student_id ORDER BY score ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS score_avg_1_prior,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY TO_DATE(enrolled_at), student_id ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_2
    FROM recent_enrollments
    WINDOW w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at NULLS LAST), w_enrolled_at_to_date_student AS (PARTITION BY TO_DATE(enrolled_at), student_id ORDER BY score ASC, id), w_enrolled_at_year_term AS (PARTITION BY YEAR(enrolled_at), term)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        term,
        enrolled_at,
        seq_no,
        prev_enrolled_at,
        second_score,
        score_sum_2_prior,
        score_share_of_enrolled_at_year_term,
        next_score,
        student_id_max_score,
        distinct_course_id_in_enrolled_at_year_term,
        score_avg_1_prior,
        second_enrolled_at_2,
        FIRST_VALUE(second_score) IGNORE NULLS OVER (PARTITION BY seq_no ORDER BY second_enrolled_at_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_second_score,
        FIRST_VALUE(seq_no) OVER (PARTITION BY seq_no, course_id ORDER BY second_enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_seq_no,
        ROW_NUMBER() OVER (PARTITION BY seq_no, course_id ORDER BY second_enrolled_at NULLS LAST, id) AS seq_no_2
    FROM ranked
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    enrolled_at,
    seq_no,
    second_score,
    score_sum_2_prior,
    score_share_of_enrolled_at_year_term,
    next_score,
    distinct_course_id_in_enrolled_at_year_term,
    score_avg_1_prior,
    second_enrolled_at_2,
    first_known_second_score,
    first_seq_no,
    seq_no + score_avg_1_prior AS seq_no_plus_score_avg_1_prior,
    COALESCE(seq_no_2, 0) AS seq_no_2_filled
FROM windowed
ORDER BY seq_no DESC NULLS FIRST, first_known_second_score DESC
