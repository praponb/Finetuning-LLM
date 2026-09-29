WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('HOUR', enrolled_at) AS enrolled_at_hour,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE course_id >= 4
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(score_filled, 2) OVER w_grade_status_by_enrolled_at AS next2_score_filled,
        NTH_VALUE(enrolled_at_hour, 2) OVER (PARTITION BY grade_status, YEAR(enrolled_at_hour) ORDER BY enrolled_at_hour DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_hour,
        SIZE(COLLECT_SET(student_id) OVER w_grade_status) AS distinct_student_id_in_grade_status,
        LEAD(enrolled_at_hour, 2) OVER (PARTITION BY grade_status, YEAR(enrolled_at_hour) ORDER BY enrolled_at_hour DESC, id) AS next2_enrolled_at_hour,
        COUNT(*) OVER (PARTITION BY grade_status) AS rows_in_grade_status
    FROM recent
    WINDOW w_grade_status_by_enrolled_at AS (PARTITION BY grade_status ORDER BY enrolled_at_hour, id), w_grade_status AS (PARTITION BY grade_status)
),
course_id_stats AS (
    -- Avg rows_in_grade_status per course_id
    SELECT course_id, AVG(rows_in_grade_status) AS avg_rows_in_grade_status
    FROM with_trends
    GROUP BY course_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_rows_in_grade_status,
        ROUND(s.rows_in_grade_status - g.avg_rows_in_grade_status, 2) AS rows_in_grade_status_minus_avg
    FROM with_trends s
    JOIN course_id_stats g ON g.course_id = s.course_id
    WHERE s.rows_in_grade_status > g.avg_rows_in_grade_status
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count,
        (SELECT COUNT(*) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_count_2
    FROM compared s
)
SELECT
    enrollment_count_2,
    student_id,
    grade_status,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT grade_status) AS distinct_grade_status
FROM with_stats
GROUP BY GROUPING SETS ((enrollment_count_2, student_id, grade_status), (enrollment_count_2), (student_id), ())
