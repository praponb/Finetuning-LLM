WITH valid_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown
    FROM enrollments
    WHERE course_id <= 3
),
with_details_course AS (
    -- Join courses
    SELECT
        s.*,
        c.credits AS course_credits,
        st.is_international AS student_is_international,
        st.major AS student_major
    FROM valid_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_enrolled_at_year_student_is AS rows_in_enrolled_at_year_student_is_internationa,
        LEAD(enrolled_at, 2) OVER (PARTITION BY course_credits ORDER BY course_credits DESC NULLS FIRST, id) AS next2_enrolled_at,
        LAG(score, 3) OVER (PARTITION BY course_credits ORDER BY course_credits DESC NULLS FIRST, id) AS prev3_score,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY grade_status, course_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        SUM(score) OVER (PARTITION BY grade_status_or_unknown ORDER BY enrolled_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS score_sum_6_prior,
        ROUND(PERCENT_RANK() OVER w_course_credits_by_course_cre, 3) AS course_credits_percent_rank,
        LEAD(enrolled_at, 2) OVER (PARTITION BY grade_status, student_major ORDER BY score, id) AS next2_enrolled_at_2,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_credits ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_2,
        ROUND(AVG(score) OVER (PARTITION BY course_credits ORDER BY course_credits DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS score_avg_4
    FROM with_details_course
    WINDOW w_enrolled_at_year_student_is AS (PARTITION BY YEAR(enrolled_at), student_is_international), w_course_credits_by_course_cre AS (PARTITION BY course_credits ORDER BY course_credits DESC NULLS FIRST, id)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(course_credits, score_sum_6_prior) AS greatest_course_credits_score_sum_6_prior,
        COALESCE(score_avg_4, 0.0) AS score_avg_4_filled
    FROM windowed
    WHERE course_credits < 4
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        grade_status_or_unknown,
        student_is_international,
        rows_in_enrolled_at_year_student_is_internationa,
        next2_enrolled_at,
        prev3_score,
        second_enrolled_at,
        score_sum_6_prior,
        course_credits_percent_rank,
        next2_enrolled_at_2,
        second_enrolled_at_2,
        score_avg_4,
        score_avg_4_filled,
        ROW_NUMBER() OVER w_all_by_score_sum_6_prior AS seq_no,
        ROUND(PERCENT_RANK() OVER (ORDER BY score_sum_6_prior DESC), 3) AS score_sum_6_prior_percent_rank,
        ROUND(AVG(course_credits_percent_rank) OVER (PARTITION BY student_is_international), 2) AS student_is_international_avg_course_credits_perc,
        NTH_VALUE(second_enrolled_at, 2) OVER (ORDER BY score_sum_6_prior DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_enrolled_at
    FROM scored
    WINDOW w_all_by_score_sum_6_prior AS (ORDER BY score_sum_6_prior DESC, id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    enrolled_at,
    grade_status_or_unknown,
    rows_in_enrolled_at_year_student_is_internationa,
    next2_enrolled_at,
    prev3_score,
    second_enrolled_at,
    score_sum_6_prior,
    course_credits_percent_rank,
    second_enrolled_at_2,
    score_avg_4,
    score_avg_4_filled,
    seq_no,
    student_is_international_avg_course_credits_perc,
    second_second_enrolled_at,
    LOWER(REGEXP_REPLACE(TRIM(grade_status_or_unknown), ' ', '_')) AS grade_status_or_unknown_slug,
    ABS(score - 35.8) AS score_distance
FROM with_trends
ORDER BY student_is_international_avg_course_credits_perc
