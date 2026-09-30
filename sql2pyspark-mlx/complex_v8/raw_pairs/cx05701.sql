WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        enrolled_at + INTERVAL 15 MINUTES AS enrolled_at_plus_15m,
        CASE WHEN score >= 57.8 THEN 'large' WHEN score >= 20.6 THEN 'medium' ELSE 'small' END AS score_tier
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND HOUR(enrolled_at) BETWEEN 10 AND 15
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_plus_15m,
        s.score_tier,
        st.enrolled_on AS student_enrolled_on,
        st.major AS student_major,
        c.dept AS course_dept
    FROM valid_enrollments s
    LEFT JOIN students st ON st.id = s.student_id
    LEFT JOIN courses c ON c.id = s.course_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        enrolled_at_plus_15m,
        score_tier,
        student_enrolled_on,
        student_major,
        LAG(student_enrolled_on) OVER w_student_major_by_student_enr AS prev_student_enrolled_on,
        ROW_NUMBER() OVER (PARTITION BY student_major ORDER BY student_enrolled_on ASC NULLS LAST, id) AS rn,
        LAG(enrolled_at_plus_15m) OVER (PARTITION BY MONTH(enrolled_at), student_id ORDER BY student_enrolled_on, id) AS prev_enrolled_at_plus_15m,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at ASC NULLS LAST, id), 3) AS enrolled_at_cume_dist,
        LAST_VALUE(student_enrolled_on) IGNORE NULLS OVER (PARTITION BY MONTH(enrolled_at), student_id ORDER BY student_enrolled_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_student_enrolled_on,
        NTH_VALUE(enrolled_at_plus_15m, 2) OVER (ORDER BY enrolled_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_plus_15m,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(enrolled_at), student_id ORDER BY student_enrolled_on, id), 3) AS student_enrolled_on_percent_rank,
        FIRST_VALUE(enrolled_at_plus_15m) OVER (ORDER BY enrolled_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at_plus_15m,
        DATEDIFF(student_enrolled_on, LAG(student_enrolled_on) OVER (PARTITION BY student_major ORDER BY student_enrolled_on ASC NULLS LAST, id)) AS days_since_prev_student_enrolled_on,
        DENSE_RANK() OVER (PARTITION BY MONTH(enrolled_at), student_id ORDER BY student_enrolled_on, id) AS student_enrolled_on_dense_rank,
        FIRST_VALUE(student_enrolled_on) OVER (PARTITION BY MONTH(enrolled_at), student_id ORDER BY student_enrolled_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_student_enrolled_on
    FROM with_parent
    WINDOW w_student_major_by_student_enr AS (PARTITION BY student_major ORDER BY student_enrolled_on ASC NULLS LAST, id)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        grade_status,
        enrolled_at,
        enrolled_at_plus_15m,
        score_tier,
        prev_enrolled_at_plus_15m,
        enrolled_at_cume_dist,
        last_known_student_enrolled_on,
        second_enrolled_at_plus_15m,
        days_since_prev_student_enrolled_on,
        student_enrolled_on_dense_rank,
        TO_DATE(enrolled_at_plus_15m) AS enrolled_at_plus_15m_date,
        GREATEST(enrolled_at_cume_dist, rn) AS greatest_enrolled_at_cume_dist_rn
    FROM metrics
    WHERE enrolled_at < TIMESTAMP '2025-02-12 09:00:00'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(enrolled_at_cume_dist) OVER (ORDER BY prev_enrolled_at_plus_15m, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at_cume_dist,
        DENSE_RANK() OVER (PARTITION BY score_tier, grade_status ORDER BY enrolled_at_cume_dist NULLS LAST) AS enrolled_at_cume_dist_dense_rank,
        DENSE_RANK() OVER (ORDER BY prev_enrolled_at_plus_15m, id) AS prev_enrolled_at_plus_15m_dense_rank,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at_plus_15m, id), 3) AS enrolled_at_plus_15m_cume_dist
    FROM enriched
),
stats AS (
    -- Summarise per group
    SELECT
        TO_DATE(prev_enrolled_at_plus_15m) AS prev_enrolled_at_plus_15m_day,
        course_id,
        student_enrolled_on_dense_rank,
        DATEDIFF(MAX(last_known_student_enrolled_on), MIN(last_known_student_enrolled_on)) AS last_known_student_enrolled_on_span_days,
        SUM(CASE WHEN grade_status = 'INCOMPLETE' THEN 1 ELSE 0 END) AS n_grade_status_incomplete,
        SUM(days_since_prev_student_enrolled_on) FILTER (WHERE grade_status = 'WITHDRAWN') AS days_since_prev_student_enrolled_on_grade_status,
        MAX(enrolled_at_plus_15m_cume_dist) AS max_enrolled_at_plus_15m_cume_dist
    FROM ranked
    WHERE days_since_prev_student_enrolled_on >= 70
    GROUP BY 1, 2, 3
),
ranked_2 AS (
    -- Rank and compare rows within groups
    SELECT
        prev_enrolled_at_plus_15m_day,
        course_id,
        student_enrolled_on_dense_rank,
        n_grade_status_incomplete,
        days_since_prev_student_enrolled_on_grade_status,
        max_enrolled_at_plus_15m_cume_dist,
        COUNT(*) OVER (PARTITION BY student_enrolled_on_dense_rank, course_id) AS rows_in_student_enrolled_on_dense_rank_course_id,
        NTILE(3) OVER w_all_by_n_grade_status_incomp AS n_grade_status_incomplete_tercile,
        SUM(n_grade_status_incomplete) OVER (ORDER BY n_grade_status_incomplete DESC, prev_enrolled_at_plus_15m_day, course_id, student_enrolled_on_dense_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_n_grade_status_incomplete,
        SUM(n_grade_status_incomplete) OVER (PARTITION BY student_enrolled_on_dense_rank, course_id) AS student_enrolled_on_dense_rank_course_id_sum_n_g
    FROM stats
    WINDOW w_all_by_n_grade_status_incomp AS (ORDER BY n_grade_status_incomplete DESC, prev_enrolled_at_plus_15m_day, course_id, student_enrolled_on_dense_rank)
)
SELECT
    prev_enrolled_at_plus_15m_day,
    course_id,
    student_enrolled_on_dense_rank,
    n_grade_status_incomplete,
    days_since_prev_student_enrolled_on_grade_status,
    max_enrolled_at_plus_15m_cume_dist,
    running_n_grade_status_incomplete
FROM ranked_2
ORDER BY course_id DESC
