WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        YEAR(enrolled_at) AS enrolled_at_year
    FROM enrollments
    WHERE student_id IS NOT NULL
),
summary AS (
    -- Group-level totals
    SELECT
        student_id,
        grade_status,
        ROUND(SUM(score) / NULLIF(SUM(enrolled_at_year), 0), 3) AS score_to_enrolled_at_year_ratio,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT term) AS distinct_terms,
        ROUND(STDDEV(score), 2) AS stddev_score
    FROM valid_enrollments
    WHERE grade_status <> 'INCOMPLETE'
    GROUP BY 1, 2
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE e.term = '2025T1') FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_term_count,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at
    FROM summary s
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.score < 63.9
              AND e.enrolled_at < TIMESTAMP '2025-02-12 10:00:00'
        )
)
SELECT student_id, grade_status, measure, amount
FROM (
    SELECT student_id, grade_status, stddev_score, score_to_enrolled_at_year_ratio
    FROM qualified
) src
UNPIVOT (
    amount FOR measure IN (stddev_score, score_to_enrolled_at_year_ratio)
) unp
