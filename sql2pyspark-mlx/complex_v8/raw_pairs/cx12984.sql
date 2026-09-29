WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(term)) AS term_len,
        ABS(score - 58.8) AS score_distance
    FROM enrollments
    WHERE course_id <= 2
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(term)) AS term_len_2,
        enrolled_at + INTERVAL 15 MINUTES AS enrolled_at_plus_15m,
        ROUND(score_distance * 0.25, 2) AS score_distance_adj
    FROM valid_enrollments
    WHERE course_id IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        score_distance,
        term_len_2,
        enrolled_at_plus_15m,
        SUM(term_len_2) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at_plus_15m) ORDER BY term_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_term_len_2,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at_plus_15m) ORDER BY term_len DESC) AS term_len_rank,
        COUNT(*) OVER (PARTITION BY YEAR(enrolled_at)) AS rows_in_enrolled_at_year,
        score_distance - LAG(score_distance) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at_plus_15m) ORDER BY term_len DESC, id) AS score_distance_change,
        ROUND(AVG(term_len_2) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at_plus_15m) ORDER BY term_len DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS term_len_2_avg_2,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY YEAR(enrolled_at))) AS distinct_term_in_enrolled_at_year
    FROM tagged
),
joined_course AS (
    -- Join courses
    SELECT
        s.*,
        c.level AS course_level,
        c.dept AS course_dept,
        c.credits AS course_credits
    FROM with_trends s
    LEFT JOIN courses c ON c.id = s.course_id
)
SELECT
    course_credits,
    course_dept,
    distinct_term_in_enrolled_at_year,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(score_distance, 0.5) AS median_score_distance
FROM joined_course
GROUP BY GROUPING SETS ((course_credits, course_dept, distinct_term_in_enrolled_at_year), (course_credits))
ORDER BY course_credits NULLS LAST, course_dept NULLS LAST, distinct_term_in_enrolled_at_year NULLS LAST
