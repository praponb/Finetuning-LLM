WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE (course_id <= 4 OR score BETWEEN 5.6 AND 59.8)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        term,
        score,
        grade_status,
        term_ref,
        grade_status_len,
        NTILE(3) OVER w_term_course_id_by_score AS score_tercile,
        SUM(score) OVER (PARTITION BY student_id) AS student_id_sum_score
    FROM filtered_enrollments
    WINDOW w_term_course_id_by_score AS (PARTITION BY term, course_id ORDER BY score DESC, id)
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM sequenced
    WHERE score > (SELECT MIN(score) FROM sequenced)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        term,
        term_ref,
        grade_status_len,
        student_id_sum_score,
        ROUND(grade_status_len - AVG(grade_status_len) OVER (PARTITION BY score_tercile, grade_status_len), 2) AS grade_status_len_vs_score_tercile_grade_status_l,
        SUM(grade_status_len) OVER w_score_tercile_grade_status_l AS score_tercile_grade_status_len_sum_grade_status_,
        MAX(student_id_sum_score) OVER (PARTITION BY score_tercile, grade_status_len) AS score_tercile_grade_status_len_max_student_id_su,
        ROUND(student_id_sum_score - AVG(student_id_sum_score) OVER (PARTITION BY score_tercile, grade_status_len), 2) AS student_id_sum_score_vs_score_tercile_grade_stat,
        SIZE(COLLECT_SET(term) OVER w_score_tercile_grade_status_l) AS distinct_term_in_score_tercile_grade_status_len,
        LEAD(score_tercile, 1, 0) OVER (PARTITION BY term ORDER BY grade_status_len DESC, id) AS next_score_tercile
    FROM filtered
    WINDOW w_score_tercile_grade_status_l AS (PARTITION BY score_tercile, grade_status_len)
)
SELECT
    id,
    term,
    term_ref,
    grade_status_len,
    student_id_sum_score,
    score_tercile_grade_status_len_max_student_id_su,
    student_id_sum_score_vs_score_tercile_grade_stat,
    distinct_term_in_score_tercile_grade_status_len,
    next_score_tercile,
    CONCAT(term, '#', CAST(id AS STRING)) AS term_ref_2
FROM metrics
ORDER BY term_ref_2
