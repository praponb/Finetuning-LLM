WITH recent AS (
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
    WHERE MONTH(enrolled_at) IN (1, 3)
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.term = '2024T2'
        )
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY student_id, term ORDER BY score DESC, id) AS score_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(enrolled_at) ORDER BY enrolled_at), 3) AS enrolled_at_cume_dist,
        MIN(score) OVER (PARTITION BY student_id, grade_status) AS student_id_grade_status_min_score,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY student_id, term ORDER BY score DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        RANK() OVER w_all_by_enrolled_at_ties AS enrolled_at_rank,
        MIN(score) OVER (PARTITION BY student_id, term ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far
    FROM without_matches
    WINDOW w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at ASC)
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM windowed s
)
-- Subtotals with ROLLUP
SELECT
    course_id,
    term,
    GROUPING(course_id) AS is_total_course_id,
    COUNT(*) AS n_rows,
    MIN(enrolled_at) AS first_enrolled_at,
    MIN(min_score_so_far) AS min_min_score_so_far
FROM with_stats
GROUP BY ROLLUP(course_id, term)
ORDER BY course_id NULLS LAST, term NULLS LAST
