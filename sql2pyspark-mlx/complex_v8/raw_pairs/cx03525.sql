WITH cleaned AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        ABS(score - 55.6) AS score_distance
    FROM enrollments
    WHERE grade_status NOT IN ('WITHDRAWN', 'FAIL')
      AND term <> '2024T1'
),
combined_details_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.score_distance,
        st.major AS student_major,
        c.course_code
    FROM cleaned s
    JOIN students st ON st.id = s.student_id
    JOIN courses c ON c.id = s.course_id
),
term_stats AS (
    -- Avg score_distance per term
    SELECT term, AVG(score_distance) AS avg_score_distance
    FROM combined_details_student
    GROUP BY term
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_score_distance,
        ROUND(s.score_distance - g.avg_score_distance, 2) AS score_distance_minus_avg
    FROM combined_details_student s
    JOIN term_stats g ON g.term = s.term
    WHERE s.score_distance > g.avg_score_distance
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM above_average s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
        )
)
-- Subtotals with GROUPING SETS
SELECT
    term,
    student_id,
    GROUPING(term) AS is_total_term,
    ROUND(SUM(avg_score_distance) / NULLIF(SUM(score_distance), 0), 3) AS avg_score_distance_to_score_distance_ratio,
    ROUND(SUM(score_distance_minus_avg), 2) AS total_score_distance_minus_avg
FROM without_matches
GROUP BY GROUPING SETS ((term, student_id), (term), ())
ORDER BY term NULLS LAST, student_id NULLS LAST
