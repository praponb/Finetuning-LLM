WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE (course_id >= 2 OR grade_status IN ('INCOMPLETE', 'FAIL'))
),
enriched_course AS (
    -- Join courses
    SELECT s.*, c.course_code, c.dept AS course_dept
    FROM valid_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        grade_status,
        COUNT(*) FILTER (WHERE term <> '2025T2') AS n_term_2025t2,
        COUNT(*) AS n_rows,
        SUM(score) AS total_score
    FROM enriched_course
    WHERE student_id IS NOT NULL
    GROUP BY grade_status
),
annotated AS (
    -- Label and score rows
    SELECT
        grade_status,
        n_term_2025t2,
        n_rows,
        FLOOR(n_term_2025t2 / 2) * 2 AS n_term_2025t2_bucket,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        total_score - n_term_2025t2 AS total_score_minus_n_term_2025t2
    FROM rollup_base
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM annotated
    WHERE grade_status_len > (SELECT MIN(grade_status_len) FROM annotated)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(n_term_2025t2) OVER (PARTITION BY n_rows) AS n_rows_sum_n_term_2025t2,
        LAG(n_term_2025t2_bucket) OVER (ORDER BY n_term_2025t2, grade_status) AS prev_n_term_2025t2_bucket
    FROM filtered
)
SELECT
    grade_status,
    n_term_2025t2,
    n_rows,
    n_term_2025t2_bucket,
    grade_status_len,
    total_score_minus_n_term_2025t2,
    n_rows_sum_n_term_2025t2,
    prev_n_term_2025t2_bucket
FROM ranked
ORDER BY n_rows_sum_n_term_2025t2 DESC
