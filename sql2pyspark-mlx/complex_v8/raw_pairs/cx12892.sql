WITH prepared AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT_WS(' / ', term, grade_status) AS term_grade_status_label,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        ROUND(score * 0.25, 2) AS score_adj
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-19 11:00:00'
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        enrolled_at + INTERVAL 7 DAYS AS enrolled_at_plus_7d,
        ABS(score_adj - 97.5) AS score_adj_distance
    FROM prepared
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM labeled s
    WHERE s.student_id IN (
            SELECT e.student_id
            FROM enrollments e
            WHERE HOUR(e.enrolled_at) BETWEEN 8 AND 14
        )
),
with_parent_student AS (
    -- Enrich with dimension data
    SELECT s.*, st.enrolled_on AS student_enrolled_on, st.cohort AS student_cohort
    FROM with_activity s
    JOIN students st ON st.id = s.student_id
)
SELECT
    grade_status_len,
    course_id,
    GROUPING(grade_status_len) AS is_total_grade_status_len,
    SUM(score_adj_distance) AS total_score_adj_distance,
    COUNT(*) FILTER (WHERE student_cohort = 'C2021') AS n_student_cohort_c2021,
    ROUND(SUM(score_adj_distance), 2) AS total_score_adj_distance_2
FROM with_parent_student
GROUP BY GROUPING SETS ((grade_status_len, course_id), (grade_status_len), (course_id), ())
