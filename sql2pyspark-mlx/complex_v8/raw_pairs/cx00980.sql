WITH recent_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        enrolled_at + INTERVAL 7 DAYS AS enrolled_at_plus_7d
    FROM enrollments
    WHERE grade_status <> 'INCOMPLETE'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_term_enrolled_at_year_by_enr, 3) AS enrolled_at_percent_rank,
        ROW_NUMBER() OVER w_student_id_grade_status_by_e AS enrolled_at_seq,
        NTH_VALUE(score_filled, 2) OVER (PARTITION BY course_id ORDER BY enrolled_at_plus_7d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_filled,
        MAX(score_filled) OVER (PARTITION BY grade_status, TO_DATE(enrolled_at_plus_7d)) AS grade_status_enrolled_at_plus_7d_to_date_max_sco
    FROM recent_enrollments
    WINDOW w_term_enrolled_at_year_by_enr AS (PARTITION BY term, YEAR(enrolled_at) ORDER BY enrolled_at ASC), w_student_id_grade_status_by_e AS (PARTITION BY student_id, grade_status ORDER BY enrolled_at DESC NULLS FIRST, id)
),
with_parent_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at,
        s.score_filled,
        s.enrolled_at_plus_7d,
        s.enrolled_at_seq,
        s.second_score_filled,
        c.level AS course_level
    FROM sequenced s
    LEFT JOIN courses c ON c.id = s.course_id
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT
        student_id,
        COUNT(*) FILTER (WHERE grade_status IN ('FAIL', 'INCOMPLETE')) AS enrollment_grade_status_count,
        MAX(enrolled_at) AS last_enrollment_enrolled_at,
        COUNT(*) AS enrollment_count
    FROM enrollments
    GROUP BY student_id
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count,
        e.last_enrollment_enrolled_at,
        COALESCE(e.enrollment_count, 0) AS enrollment_count
    FROM with_parent_course s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS('-', term, course_level) AS term_course_level_label,
        TO_DATE(enrolled_at_plus_7d) AS enrolled_at_plus_7d_date
    FROM with_counts
),
with_parent AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.enrolled_at_plus_7d,
        s.second_score_filled,
        s.course_level,
        s.enrollment_grade_status_count,
        s.last_enrollment_enrolled_at,
        s.enrollment_count,
        s.term_course_level_label,
        s.enrolled_at_plus_7d_date,
        c.credits AS course_credits,
        c.course_code,
        st.enrolled_on AS student_enrolled_on
    FROM enriched s
    JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at_plus_7d,
        second_score_filled,
        course_level,
        enrollment_grade_status_count,
        last_enrollment_enrolled_at,
        enrollment_count,
        term_course_level_label,
        enrolled_at_plus_7d_date,
        course_credits,
        course_code,
        student_enrolled_on,
        RANK() OVER (PARTITION BY term, enrollment_count ORDER BY second_score_filled, id) AS second_score_filled_rank,
        SUM(course_credits) OVER (PARTITION BY course_id, enrollment_grade_status_count ORDER BY enrollment_count DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS course_credits_sum_3_prior,
        NTILE(5) OVER (PARTITION BY term, enrollment_count ORDER BY second_score_filled, id) AS second_score_filled_quintile,
        RANK() OVER (PARTITION BY enrollment_grade_status_count ORDER BY enrolled_at_plus_7d_date DESC NULLS FIRST, id) AS enrolled_at_plus_7d_date_rank
    FROM with_parent
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    enrolled_at_plus_7d,
    course_level,
    enrollment_grade_status_count,
    last_enrollment_enrolled_at,
    enrollment_count,
    term_course_level_label,
    course_credits,
    course_code,
    second_score_filled_rank,
    course_credits_sum_3_prior,
    enrolled_at_plus_7d_date_rank,
    DATE_FORMAT(enrolled_at_plus_7d, 'MMM yyyy') AS enrolled_at_plus_7d_label
FROM with_windows
ORDER BY last_enrollment_enrolled_at, second_score_filled_rank
