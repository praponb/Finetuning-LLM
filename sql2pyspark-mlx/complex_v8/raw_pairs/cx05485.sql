WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        ABS(score - 54.2) AS score_distance,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-02-06 13:00:00'
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM valid_enrollments
    WHERE score_distance < (SELECT MAX(score_distance) FROM valid_enrollments)
),
grade_status_stats AS (
    -- Avg score_distance per grade_status
    SELECT grade_status, AVG(score_distance) AS avg_score_distance
    FROM screened
    GROUP BY grade_status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_score_distance,
        ROUND(s.score_distance - g.avg_score_distance, 2) AS score_distance_minus_avg
    FROM screened s
    JOIN grade_status_stats g ON g.grade_status = s.grade_status
    WHERE s.score_distance > g.avg_score_distance
),
with_parent AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.score_distance,
        s.term_or_unknown,
        s.avg_score_distance,
        s.score_distance_minus_avg,
        st.major AS student_major,
        st.gpa AS student_gpa,
        c.credits AS course_credits
    FROM outliers s
    JOIN students st ON st.id = s.student_id AND st.gpa >= 2.7
    JOIN courses c ON c.id = s.course_id
    WHERE st.major = 'CS'
)
-- Final projection
SELECT
    id,
    student_id,
    term_or_unknown,
    avg_score_distance,
    score_distance_minus_avg,
    student_major,
    student_gpa,
    course_credits,
    ROUND(score_distance / NULLIF(avg_score_distance, 0), 3) AS score_distance_per_avg_score_distance,
    CASE WHEN score_distance_minus_avg >= 20.0 THEN 'HIGH' WHEN score_distance_minus_avg >= -40.0 THEN 'MEDIUM' ELSE 'LOW' END AS score_distance_minus_avg_tier
FROM with_parent
ORDER BY student_gpa DESC NULLS FIRST, avg_score_distance DESC NULLS FIRST
