WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE grade_status IN ('INCOMPLETE', 'PASS', 'FAIL')
      AND TO_DATE(enrolled_at) < DATE '2025-03-08'
),
with_parent_student AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_label,
        s.grade_status_or_unknown,
        st.major AS student_major,
        st.enrolled_on AS student_enrolled_on
    FROM filtered s
    JOIN students st ON st.id = s.student_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(score) OVER (PARTITION BY student_major, grade_status_or_unknown ORDER BY student_enrolled_on NULLS LAST, id) AS next_score,
        RANK() OVER w_student_major_grade_status_o_2 AS student_enrolled_on_rank,
        SUM(score) OVER (ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        MAX(score) OVER (PARTITION BY student_major, student_id) AS student_major_student_id_max_score,
        RANK() OVER (PARTITION BY course_id, student_major ORDER BY student_enrolled_on) AS student_enrolled_on_rank_2,
        ROUND(CUME_DIST() OVER w_all_by_enrolled_at_ties, 3) AS enrolled_at_cume_dist,
        ROUND(PERCENT_RANK() OVER w_all_by_enrolled_at, 3) AS enrolled_at_percent_rank,
        ROUND(score / NULLIF(SUM(score) OVER w_student_major_student_id, 0), 4) AS score_share_of_student_major_student_id,
        ROW_NUMBER() OVER (PARTITION BY student_major, grade_status_or_unknown ORDER BY student_enrolled_on NULLS LAST, id) AS student_enrolled_on_seq,
        NTILE(3) OVER (PARTITION BY course_id, student_major ORDER BY student_enrolled_on, id) AS student_enrolled_on_tercile,
        ROUND(AVG(score) OVER (ORDER BY score NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS score_avg_1_prior
    FROM with_parent_student
    WINDOW w_student_major_grade_status_o_2 AS (PARTITION BY student_major, grade_status_or_unknown ORDER BY student_enrolled_on NULLS LAST), w_all_by_enrolled_at_ties AS (ORDER BY enrolled_at), w_all_by_enrolled_at AS (ORDER BY enrolled_at, id), w_student_major_student_id AS (PARTITION BY student_major, student_id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(enrolled_at) AS enrolled_at_day,
        student_enrolled_on_tercile,
        grade_status_or_unknown,
        MAX(enrolled_at) AS last_enrolled_at,
        MAX(student_enrolled_on) AS last_student_enrolled_on,
        ARRAY_SORT(COLLECT_SET(term)) AS term_list,
        COUNT(*) FILTER (WHERE grade_status = 'WITHDRAWN') AS n_grade_status_withdrawn,
        ROUND(STDDEV(student_enrolled_on_tercile), 2) AS stddev_student_enrolled_on_tercile,
        ROUND(SUM(student_enrolled_on_rank) / NULLIF(SUM(enrolled_at_cume_dist), 0), 3) AS student_enrolled_on_rank_to_enrolled_at_cume_dis
    FROM ranked
    GROUP BY 1, 2, 3
)
SELECT
    enrolled_at_day,
    grade_status_or_unknown,
    term_list,
    n_grade_status_withdrawn,
    student_enrolled_on_rank_to_enrolled_at_cume_dis,
    COALESCE(n_grade_status_withdrawn, 0) AS n_grade_status_withdrawn_filled
FROM stats
