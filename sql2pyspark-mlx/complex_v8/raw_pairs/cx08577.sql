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
        DATE_TRUNC('WEEK', enrolled_at) AS enrolled_at_week
    FROM enrollments
    WHERE (score <= 79.2 OR MONTH(enrolled_at) IN (1, 2))
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        course_id,
        COUNT(*) FILTER (WHERE grade_status = 'INCOMPLETE') AS n_grade_status_incomplete,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        MAX(score) AS max_score
    FROM valid
    GROUP BY course_id
    HAVING AVG(score) > 10.8
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        n_rows - LAG(n_rows) OVER (PARTITION BY course_id ORDER BY n_grade_status_incomplete, course_id) AS n_rows_change,
        NTILE(3) OVER w_course_id_by_n_grade_status AS n_grade_status_incomplete_tercile
    FROM grouped
    WINDOW w_course_id_by_n_grade_status AS (PARTITION BY course_id ORDER BY n_grade_status_incomplete, course_id)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        course_id,
        n_rows,
        distinct_student_ids,
        max_score,
        n_rows_change,
        n_grade_status_incomplete_tercile,
        LAST_VALUE(n_rows_change) IGNORE NULLS OVER (PARTITION BY distinct_student_ids, course_id ORDER BY n_rows_change, course_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_n_rows_change,
        ROUND(n_grade_status_incomplete / NULLIF(SUM(n_grade_status_incomplete) OVER (PARTITION BY course_id, n_grade_status_incomplete_tercile), 0), 4) AS n_grade_status_incomplete_share_of_course_id_n_g,
        LEAD(max_score, 2) OVER (ORDER BY max_score, course_id) AS next2_max_score,
        COUNT(*) OVER (PARTITION BY distinct_student_ids, course_id ORDER BY n_rows_change, course_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM with_trends
)
-- Final projection
SELECT
    course_id,
    n_rows,
    distinct_student_ids,
    max_score,
    n_grade_status_incomplete_tercile,
    last_known_n_rows_change,
    n_grade_status_incomplete_share_of_course_id_n_g,
    running_count,
    ABS(max_score - 65.0) AS max_score_distance
FROM metrics
ORDER BY max_score_distance NULLS LAST, last_known_n_rows_change DESC
