WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE grade_status IN ('INCOMPLETE', 'WITHDRAWN')
      AND score >= 11.3
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        score,
        ROUND(AVG(score) OVER w_student_id_grade_status, 2) AS student_id_grade_status_avg_score,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        ROW_NUMBER() OVER w_all_by_enrolled_at AS seq_no
    FROM cleaned_enrollments
    WINDOW w_student_id_grade_status AS (PARTITION BY student_id, grade_status), w_all_by_enrolled_at AS (ORDER BY enrolled_at ASC, id)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        seq_no,
        student_id,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        MAX(seq_no) AS max_seq_no,
        ROUND(SUM(score), 2) AS total_score,
        ROUND(STDDEV(score), 2) AS stddev_score,
        ROUND(AVG(student_id_grade_status_avg_score), 2) AS avg_student_id_grade_status_avg_score,
        MAX(student_id_grade_status_avg_score) AS max_student_id_grade_status_avg_score
    FROM ranked
    GROUP BY seq_no, student_id
),
enriched_student AS (
    -- Enrich with dimension data
    SELECT s.*, st.enrolled_on AS student_enrolled_on, st.major AS student_major
    FROM totals s
    LEFT JOIN students st ON st.id = s.student_id
)
SELECT
    seq_no,
    student_id,
    distinct_student_ids,
    total_score,
    avg_student_id_grade_status_avg_score,
    max_student_id_grade_status_avg_score,
    student_enrolled_on,
    student_major
FROM enriched_student
ORDER BY total_score
