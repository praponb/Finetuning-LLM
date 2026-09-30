WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 0.9, 2) AS score_adj
    FROM enrollments
    WHERE student_id >= 6
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        term,
        score,
        grade_status,
        ROW_NUMBER() OVER (PARTITION BY grade_status, term ORDER BY score_adj DESC, id) AS rn,
        ROUND(CUME_DIST() OVER w_grade_status_term_by_score_a, 3) AS score_adj_cume_dist
    FROM valid
    WINDOW w_grade_status_term_by_score_a AS (PARTITION BY grade_status, term ORDER BY score_adj DESC, id)
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.rn,
        s.score_adj_cume_dist,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.term AS enrollment_term
    FROM windowed s
    LEFT JOIN enrollments e ON e.course_id = s.course_id
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM detail_rows s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.student_id <= 19
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT term, enrollment_term, grade_status, score_adj_cume_dist
    FROM without_matches
) src
PIVOT (
    SUM(score_adj_cume_dist) AS total, COUNT(score_adj_cume_dist) AS n
    FOR grade_status IN ('INCOMPLETE', 'PASS')
)
ORDER BY term, enrollment_term
