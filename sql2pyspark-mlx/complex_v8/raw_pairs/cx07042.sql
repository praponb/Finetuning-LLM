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
        UPPER(TRIM(term)) AS term_clean
    FROM enrollments
),
with_details AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.term_clean,
        c.credits AS course_credits,
        c.level AS course_level,
        c.course_code,
        st.is_international AS student_is_international
    FROM recent_enrollments s
    JOIN courses c ON c.id = s.course_id AND c.course_code IS NOT NULL
    LEFT JOIN students st ON st.id = s.student_id
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(enrolled_at) AS enrolled_at_day,
        course_id,
        ROUND(SUM(score) / NULLIF(SUM(course_credits), 0), 3) AS score_to_course_credits_ratio,
        COUNT(DISTINCT grade_status) AS distinct_grade_status
    FROM with_details
    GROUP BY TO_DATE(enrolled_at), course_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        enrolled_at_day,
        course_id,
        distinct_grade_status,
        ROUND(distinct_grade_status / NULLIF(SUM(distinct_grade_status) OVER (PARTITION BY course_id, YEAR(enrolled_at_day)), 0), 4) AS distinct_grade_status_share_of_course_id_enrolle,
        ROUND(distinct_grade_status - AVG(distinct_grade_status) OVER (PARTITION BY course_id, YEAR(enrolled_at_day)), 2) AS distinct_grade_status_vs_course_id_enrolled_at_d,
        LAG(enrolled_at_day, 3) OVER (PARTITION BY distinct_grade_status, MONTH(enrolled_at_day) ORDER BY score_to_course_credits_ratio, enrolled_at_day, course_id) AS prev3_enrolled_at_day,
        MAX(distinct_grade_status) OVER (PARTITION BY distinct_grade_status, MONTH(enrolled_at_day) ORDER BY score_to_course_credits_ratio, enrolled_at_day, course_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS distinct_grade_status_max_2_prior,
        SUM(distinct_grade_status) OVER w_course_id_enrolled_at_day_ye AS course_id_enrolled_at_day_year_sum_distinct_grad,
        ROUND(PERCENT_RANK() OVER (ORDER BY distinct_grade_status, enrolled_at_day, course_id), 3) AS distinct_grade_status_percent_rank
    FROM per_group
    WINDOW w_course_id_enrolled_at_day_ye AS (PARTITION BY course_id, YEAR(enrolled_at_day))
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE e.grade_status IN ('WITHDRAWN', 'PASS')) FROM enrollments e WHERE e.course_id = s.course_id) AS enrollment_grade_status_count,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.course_id = s.course_id) AS last_enrollment_enrolled_at,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.course_id = s.course_id) AS max_enrollment_score
    FROM ranked s
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        enrolled_at_day,
        course_id,
        distinct_grade_status,
        distinct_grade_status_share_of_course_id_enrolle,
        distinct_grade_status_vs_course_id_enrolled_at_d,
        prev3_enrolled_at_day,
        distinct_grade_status_percent_rank,
        enrollment_grade_status_count,
        NTH_VALUE(distinct_grade_status, 2) OVER (ORDER BY distinct_grade_status_share_of_course_id_enrolle NULLS LAST, enrolled_at_day, course_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_grade_status,
        ROUND(AVG(distinct_grade_status) OVER (ORDER BY distinct_grade_status_share_of_course_id_enrolle NULLS LAST, enrolled_at_day, course_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS distinct_grade_status_avg_1_prior,
        SIZE(COLLECT_SET(course_id) OVER w_distinct_grade_status) AS distinct_course_id_in_distinct_grade_status,
        COUNT(*) OVER (PARTITION BY distinct_grade_status) AS rows_in_distinct_grade_status,
        ROUND(AVG(distinct_grade_status) OVER w_distinct_grade_status, 2) AS distinct_grade_status_avg_distinct_grade_status
    FROM with_counts
    WINDOW w_distinct_grade_status AS (PARTITION BY distinct_grade_status)
)
SELECT
    enrolled_at_day,
    course_id,
    distinct_grade_status_share_of_course_id_enrolle,
    prev3_enrolled_at_day,
    distinct_grade_status_percent_rank,
    second_distinct_grade_status,
    distinct_grade_status_avg_1_prior,
    distinct_course_id_in_distinct_grade_status,
    rows_in_distinct_grade_status,
    distinct_grade_status_avg_distinct_grade_status,
    DATEDIFF(prev3_enrolled_at_day, enrolled_at_day) AS days_enrolled_at_day_to_prev3_enrolled_at_day
FROM windowed
WHERE YEAR(enrolled_at_day) = 2024
