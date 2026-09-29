WITH recent AS (
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
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY student_id)) AS distinct_term_in_student_id,
        NTILE(5) OVER (ORDER BY score DESC, id) AS score_quintile
    FROM recent
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        score,
        distinct_term_in_student_id,
        score_quintile,
        ROW_NUMBER() OVER (ORDER BY enrolled_at DESC, id) AS seq_no,
        ROUND(score - AVG(score) OVER (PARTITION BY distinct_term_in_student_id, score_quintile), 2) AS score_vs_distinct_term_in_student_id_score_quint,
        SUM(score) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        LAG(distinct_term_in_student_id) OVER (ORDER BY enrolled_at DESC, id) AS prev_distinct_term_in_student_id,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY distinct_term_in_student_id, score_quintile)) AS distinct_grade_status_in_distinct_term_in_studen
    FROM windowed
)
SELECT
    student_id,
    score_quintile,
    prev_distinct_term_in_student_id,
    SUM(prev_distinct_term_in_student_id) AS total_prev_distinct_term_in_student_id
FROM sequenced
GROUP BY CUBE(student_id, score_quintile, prev_distinct_term_in_student_id)
