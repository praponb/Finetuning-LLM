WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE (student_id IS NOT NULL OR grade_status IS NOT NULL)
),
joined_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.enrolled_at,
        c.dept AS course_dept
    FROM valid_enrollments s
    JOIN courses c ON c.id = s.course_id
    WHERE c.dept <> 'CS'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        enrolled_at,
        course_dept,
        LAG(enrolled_at) OVER w_course_id_by_enrolled_at AS prev_enrolled_at,
        ROUND(CUME_DIST() OVER (PARTITION BY course_id, course_dept ORDER BY enrolled_at, id), 3) AS enrolled_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_course_dept_student_id_by_en)) / 60, 1) AS minutes_since_prev_enrolled_at,
        LEAD(enrolled_at, 3) OVER (PARTITION BY course_dept ORDER BY enrolled_at ASC NULLS LAST, id) AS next3_enrolled_at,
        LAG(enrolled_at) OVER (ORDER BY enrolled_at, id) AS prev_enrolled_at_2,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at,
        DENSE_RANK() OVER (PARTITION BY course_dept, student_id ORDER BY enrolled_at ASC) AS enrolled_at_dense_rank,
        NTILE(5) OVER w_all_by_enrolled_at AS enrolled_at_quintile,
        DENSE_RANK() OVER (ORDER BY enrolled_at, id) AS enrolled_at_dense_rank_2,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY course_id, course_dept ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        LAG(enrolled_at, 3) OVER w_course_id_by_enrolled_at AS prev3_enrolled_at,
        DENSE_RANK() OVER (PARTITION BY course_dept ORDER BY enrolled_at ASC NULLS LAST) AS enrolled_at_dense_rank_3
    FROM joined_course
    WINDOW w_course_id_by_enrolled_at AS (PARTITION BY course_id ORDER BY enrolled_at, id), w_course_dept_student_id_by_en AS (PARTITION BY course_dept, student_id ORDER BY enrolled_at ASC, id), w_all_by_enrolled_at AS (ORDER BY enrolled_at, id)
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(prev_enrolled_at, 'yyyy-MM') AS prev_enrolled_at_ym,
        DATE_TRUNC('MONTH', prev_enrolled_at_2) AS prev_enrolled_at_2_month,
        DATE_TRUNC('MONTH', next3_enrolled_at) AS next3_enrolled_at_month,
        MIN(prev_enrolled_at_2) AS first_prev_enrolled_at_2,
        SUM(CASE WHEN course_dept IS NOT NULL THEN 1 ELSE 0 END) AS n_course_dept_is_not_null,
        PERCENTILE_APPROX(enrolled_at_dense_rank_3, 0.5) AS median_enrolled_at_dense_rank_3,
        ROUND(STDDEV(enrolled_at_quintile), 2) AS stddev_enrolled_at_quintile
    FROM metrics
    WHERE enrolled_at_cume_dist >= 0.1
    GROUP BY DATE_FORMAT(prev_enrolled_at, 'yyyy-MM'), DATE_TRUNC('MONTH', prev_enrolled_at_2), DATE_TRUNC('MONTH', next3_enrolled_at)
)
SELECT
    prev_enrolled_at_ym,
    next3_enrolled_at_month,
    first_prev_enrolled_at_2,
    n_course_dept_is_not_null,
    stddev_enrolled_at_quintile
FROM summary
WHERE median_enrolled_at_dense_rank_3 <= 9
ORDER BY next3_enrolled_at_month, prev_enrolled_at_ym DESC NULLS FIRST
