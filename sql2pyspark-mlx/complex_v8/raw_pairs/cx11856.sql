WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        QUARTER(enrolled_at) AS enrolled_at_quarter,
        CONCAT_WS('|', grade_status, term) AS grade_status_term_label,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND course_id IS NOT NULL
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(e.score), 2) FROM enrollments e WHERE e.course_id = s.course_id) AS avg_enrollment_score
    FROM cleaned_enrollments s
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(score) OVER (PARTITION BY enrolled_at_quarter, grade_status ORDER BY avg_enrollment_score, id) AS prev_score,
        COUNT(*) OVER (PARTITION BY grade_status) AS rows_in_grade_status,
        ROUND(avg_enrollment_score / NULLIF(SUM(avg_enrollment_score) OVER (PARTITION BY grade_status), 0), 4) AS avg_enrollment_score_share_of_grade_status,
        RANK() OVER (PARTITION BY enrolled_at_quarter, grade_status ORDER BY avg_enrollment_score, id) AS avg_enrollment_score_rank,
        MIN(score) OVER (PARTITION BY enrolled_at_quarter, grade_status ORDER BY avg_enrollment_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_score_so_far,
        MAX(score) OVER (PARTITION BY grade_status) AS grade_status_max_score
    FROM with_counts
),
summary AS (
    -- Group-level totals
    SELECT
        grade_status,
        ROUND(STDDEV(enrolled_at_quarter), 2) AS stddev_enrolled_at_quarter,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list
    FROM ranked
    WHERE student_id IS NOT NULL
    GROUP BY grade_status
    HAVING AVG(score) > 34.2
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM summary
    WHERE stddev_enrolled_at_quarter > (SELECT MIN(stddev_enrolled_at_quarter) FROM summary)
),
with_activity_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM with_activity
    WHERE stddev_enrolled_at_quarter < (SELECT MAX(stddev_enrolled_at_quarter) FROM with_activity)
)
-- Final projection
SELECT grade_status, stddev_enrolled_at_quarter
FROM with_activity_2
ORDER BY stddev_enrolled_at_quarter DESC, grade_status
