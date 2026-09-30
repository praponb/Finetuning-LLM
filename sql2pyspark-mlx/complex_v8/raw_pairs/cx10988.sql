WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE term IN ('2025T1', '2025T2', '2024T1')
      AND enrolled_at >= TIMESTAMP '2025-02-06 11:00:00'
),
with_details_student AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.days_since_enrolled_at,
        s.term_or_unknown,
        st.gpa AS student_gpa
    FROM valid_enrollments s
    JOIN students st ON st.id = s.student_id
),
student_id_stats AS (
    -- Max score per student_id
    SELECT student_id, MAX(score) AS max_score
    FROM with_details_student
    GROUP BY student_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_score, ROUND(s.score - g.max_score, 2) AS score_minus_max
    FROM with_details_student s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.score = g.max_score
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(score) IGNORE NULLS OVER (PARTITION BY course_id, term ORDER BY score_minus_max DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score,
        student_gpa - LAG(student_gpa) OVER (PARTITION BY course_id ORDER BY student_gpa NULLS LAST, id) AS student_gpa_change,
        MIN(max_score) OVER w_term AS term_min_max_score,
        ROUND(CUME_DIST() OVER (PARTITION BY term, course_id ORDER BY max_score DESC NULLS FIRST, id), 3) AS max_score_cume_dist
    FROM compared
    WINDOW w_term AS (PARTITION BY term)
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at
    FROM windowed s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
)
-- Final projection
SELECT
    id,
    course_id,
    score,
    enrolled_at,
    term_or_unknown,
    max_score,
    score_minus_max,
    student_gpa_change,
    max_score_cume_dist,
    last_enrollment_enrolled_at,
    COALESCE(max_score_cume_dist, 0.0) AS max_score_cume_dist_filled,
    CASE WHEN term_or_unknown IN ('2024T1', '2025T1') THEN 'group_a' WHEN term_or_unknown = '2025T2' THEN 'group_b' ELSE 'other' END AS term_or_unknown_group
FROM summarized_rows
ORDER BY student_gpa_change, last_enrollment_enrolled_at DESC NULLS FIRST
