WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 1.1, 2) AS score_adj
    FROM enrollments
    WHERE (score > 38.1 OR course_id IS NOT NULL)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        score,
        enrolled_at,
        SUM(score) OVER w_course_id AS course_id_sum_score,
        ROUND(CUME_DIST() OVER w_course_id_grade_status_by_en, 3) AS enrolled_at_cume_dist,
        LAST_VALUE(score_adj) OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_score_adj,
        MAX(score_adj) OVER (PARTITION BY course_id) AS course_id_max_score_adj,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_id, grade_status ORDER BY enrolled_at), 3) AS enrolled_at_percent_rank
    FROM scoped_enrollments
    WINDOW w_course_id AS (PARTITION BY course_id), w_course_id_grade_status_by_en AS (PARTITION BY course_id, grade_status ORDER BY enrolled_at)
),
filtered AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND st.cohort <> 'C2024'
              AND st.gpa >= 2.7
        )
),
student_id_stats AS (
    -- Median enrolled_at_percent_rank per student_id
    SELECT student_id, PERCENTILE_APPROX(enrolled_at_percent_rank, 0.5) AS median_enrolled_at_percent_rank
    FROM filtered
    GROUP BY student_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_enrolled_at_percent_rank,
        ROUND(s.enrolled_at_percent_rank - g.median_enrolled_at_percent_rank, 2) AS enrolled_at_percent_rank_minus_median
    FROM filtered s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.enrolled_at_percent_rank >= g.median_enrolled_at_percent_rank
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.course_id_sum_score,
        s.enrolled_at_cume_dist,
        s.last_score_adj,
        s.course_id_max_score_adj,
        s.minutes_since_prev_enrolled_at,
        s.enrolled_at_percent_rank,
        s.median_enrolled_at_percent_rank,
        st.cohort AS student_cohort
    FROM vs_group s
    JOIN students st ON st.id = s.student_id
    WHERE st.cohort NOT IN ('C2021', 'C2022')
),
joined AS (
    -- Join students
    SELECT
        s.id,
        s.student_id,
        s.enrolled_at,
        s.last_score_adj,
        s.enrolled_at_percent_rank,
        s.median_enrolled_at_percent_rank,
        s.student_cohort,
        st.student_name
    FROM enriched s
    LEFT JOIN students st ON st.id = s.student_id
)
SELECT
    id,
    student_id,
    enrolled_at_percent_rank,
    median_enrolled_at_percent_rank,
    student_cohort,
    student_name
FROM joined
WHERE median_enrolled_at_percent_rank >= 0.3
ORDER BY enrolled_at_percent_rank DESC NULLS FIRST
