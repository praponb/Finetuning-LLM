WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year,
        CONCAT_WS('|', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-13 11:00:00'
),
with_details_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at_year,
        st.enrolled_on AS student_enrolled_on,
        st.cohort AS student_cohort,
        st.major AS student_major,
        c.credits AS course_credits
    FROM filtered_enrollments s
    LEFT JOIN students st ON st.id = s.student_id AND UPPER(TRIM(st.student_name)) LIKE 'L%'
    LEFT JOIN courses c ON c.id = s.course_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(student_enrolled_on) OVER (PARTITION BY student_id ORDER BY course_credits, id) AS prev_student_enrolled_on,
        LEAD(course_credits, 3) OVER (PARTITION BY course_credits, DATE_TRUNC('MONTH', student_enrolled_on) ORDER BY enrolled_at_year DESC, id) AS next3_course_credits,
        COUNT(*) OVER (PARTITION BY course_credits, course_id) AS rows_in_course_credits_course_id,
        MIN(score) OVER (PARTITION BY course_credits, course_id) AS course_credits_course_id_min_score,
        SUM(score) OVER (PARTITION BY course_credits, course_id) AS course_credits_course_id_sum_score,
        ROUND(AVG(score) OVER (PARTITION BY course_credits, DATE_TRUNC('MONTH', student_enrolled_on) ORDER BY enrolled_at_year DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS score_avg_3,
        LAG(course_credits, 1, 0) OVER (PARTITION BY student_id ORDER BY course_credits, id) AS prev_course_credits,
        ROUND(AVG(score) OVER (PARTITION BY student_id ORDER BY course_credits, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS score_avg_6_prior
    FROM with_details_student
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        course_id,
        grade_status,
        enrolled_at_year,
        student_enrolled_on,
        student_major,
        prev_student_enrolled_on,
        course_credits_course_id_min_score,
        course_credits_course_id_sum_score,
        prev_course_credits,
        score_avg_6_prior,
        TO_DATE(enrolled_at_year) AS enrolled_at_year_date,
        FLOOR(score_avg_3 / 25) * 25 AS score_avg_3_bucket,
        COALESCE(next3_course_credits, 0) AS next3_course_credits_filled
    FROM sequenced
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY prev_course_credits, next3_course_credits_filled) AS rows_in_prev_course_credits_next3_course_credits,
        score_avg_3_bucket - LAG(score_avg_3_bucket) OVER w_prev_student_enrolled_on_yea AS score_avg_3_bucket_change,
        ROUND(AVG(score_avg_6_prior) OVER w_prev_student_enrolled_on_mon, 2) AS prev_student_enrolled_on_month_avg_score_avg_6_p,
        ROUND(next3_course_credits_filled / NULLIF(SUM(next3_course_credits_filled) OVER (PARTITION BY prev_course_credits, next3_course_credits_filled), 0), 4) AS next3_course_credits_filled_share_of_prev_course,
        COUNT(*) OVER w_prev_student_enrolled_on_mon AS rows_in_prev_student_enrolled_on_month
    FROM labeled
    WINDOW w_prev_student_enrolled_on_yea AS (PARTITION BY YEAR(prev_student_enrolled_on), prev_course_credits ORDER BY score_avg_6_prior, id), w_prev_student_enrolled_on_mon AS (PARTITION BY MONTH(prev_student_enrolled_on))
)
-- Final projection
SELECT
    id,
    course_id,
    grade_status,
    student_enrolled_on,
    student_major,
    prev_student_enrolled_on,
    course_credits_course_id_min_score,
    course_credits_course_id_sum_score,
    prev_course_credits,
    enrolled_at_year_date,
    score_avg_3_bucket,
    next3_course_credits_filled,
    rows_in_prev_course_credits_next3_course_credits,
    score_avg_3_bucket_change,
    prev_student_enrolled_on_month_avg_score_avg_6_p,
    next3_course_credits_filled_share_of_prev_course,
    rows_in_prev_student_enrolled_on_month
FROM with_windows
WHERE score_avg_6_prior <= 60.0
ORDER BY score_avg_3_bucket DESC
