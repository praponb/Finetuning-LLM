WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.07, 2) AS score_adj,
        LENGTH(TRIM(term)) AS term_len,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE TO_DATE(enrolled_at) < DATE '2025-03-02'
      AND grade_status IN ('PASS', 'FAIL')
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        term,
        score,
        grade_status,
        grade_status_or_unknown,
        FLOOR(score / 25) * 25 AS score_bucket,
        LENGTH(TRIM(term)) AS term_len_2,
        ROUND(score / NULLIF(score_adj, 0), 3) AS score_per_score_adj
    FROM prepared
    WHERE score BETWEEN 10.1 AND 77.0
),
combined_details AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.term,
        s.score,
        s.grade_status,
        s.score_bucket,
        s.score_per_score_adj,
        st.enrolled_on AS student_enrolled_on
    FROM scored s
    JOIN students st ON st.id = s.student_id
    WHERE st.enrolled_on BETWEEN DATE '2024-02-28' AND DATE '2025-07-03'
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM combined_details
    WHERE score < (SELECT MAX(score) FROM combined_details)
)
SELECT id, measure, metric_value
FROM (
    SELECT id, score, score_per_score_adj
    FROM qualified
) src
UNPIVOT (
    metric_value FOR measure IN (score, score_per_score_adj)
) unp
