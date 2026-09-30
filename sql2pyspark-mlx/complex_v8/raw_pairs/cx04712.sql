WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ABS(score - 43.5) AS score_distance
    FROM enrollments
    WHERE score < 81.4
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM valid s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM profiled
    WHERE enrollment_count > (SELECT MIN(enrollment_count) FROM profiled)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id,
        ROUND(AVG(enrollment_count) OVER (PARTITION BY student_id ORDER BY score_distance, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS enrollment_count_avg_2_prior,
        LAST_VALUE(score_distance) IGNORE NULLS OVER (PARTITION BY enrollment_count ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score_distance,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id
    FROM qualified
),
student_id_stats AS (
    -- Avg score per student_id
    SELECT student_id, AVG(score) AS avg_score
    FROM sequenced
    GROUP BY student_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_score, ROUND(s.score - g.avg_score, 2) AS score_minus_avg
    FROM sequenced s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.score > g.avg_score
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        term,
        score,
        last_known_score_distance,
        rows_in_course_id,
        avg_score,
        score_minus_avg,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY term, grade_status)) AS distinct_student_id_in_term_grade_status,
        ROUND(AVG(score) OVER (PARTITION BY term, grade_status), 2) AS term_grade_status_avg_score,
        ROUND(score_share_of_course_id / NULLIF(SUM(score_share_of_course_id) OVER (PARTITION BY term, grade_status), 0), 4) AS score_share_of_course_id_share_of_term_grade_sta,
        MAX(avg_score) OVER w_term_grade_status AS term_grade_status_max_avg_score,
        COUNT(*) OVER (PARTITION BY term, grade_status) AS rows_in_term_grade_status
    FROM above_average
    WINDOW w_term_grade_status AS (PARTITION BY term, grade_status)
)
SELECT
    id,
    student_id,
    term,
    score,
    last_known_score_distance,
    rows_in_course_id,
    distinct_student_id_in_term_grade_status,
    term_grade_status_avg_score,
    score_share_of_course_id_share_of_term_grade_sta,
    term_grade_status_max_avg_score,
    rows_in_term_grade_status
FROM with_trends
ORDER BY id DESC
LIMIT 5
