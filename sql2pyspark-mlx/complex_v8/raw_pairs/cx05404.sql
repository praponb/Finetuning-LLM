WITH cleaned_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
    FROM enrollments
    WHERE (grade_status = 'FAIL' OR enrolled_at < TIMESTAMP '2025-02-18 11:00:00')
),
with_details_course AS (
    -- Attach course attributes
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        c.course_code,
        c.level AS course_level
    FROM cleaned_enrollments s
    JOIN courses c ON c.id = s.course_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_id ORDER BY enrolled_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROUND(AVG(score) OVER (ORDER BY enrolled_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7,
        LAG(score, 2) OVER (PARTITION BY course_id ORDER BY enrolled_at ASC NULLS LAST, id) AS prev2_score,
        ROUND(CUME_DIST() OVER (PARTITION BY course_id ORDER BY enrolled_at ASC NULLS LAST), 3) AS enrolled_at_cume_dist,
        ROUND(score - AVG(score) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), grade_status), 2) AS score_vs_enrolled_at_month_grade_status_avg,
        COUNT(*) OVER w_enrolled_at_month_grade_stat AS rows_in_enrolled_at_month_grade_status,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), grade_status)) AS distinct_student_id_in_enrolled_at_month_grade_s,
        SIZE(COLLECT_SET(term) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), grade_status)) AS distinct_term_in_enrolled_at_month_grade_status,
        LEAD(score) OVER w_all_by_enrolled_at AS next_score,
        MAX(score) OVER (PARTITION BY course_id ORDER BY enrolled_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_so_far,
        MAX(score) OVER w_enrolled_at_month_grade_stat AS enrolled_at_month_grade_status_max_score,
        ROUND(CUME_DIST() OVER (ORDER BY enrolled_at, id), 3) AS enrolled_at_cume_dist_2,
        LEAD(score, 3) OVER (ORDER BY enrolled_at, id) AS next3_score,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), grade_status), 0), 4) AS score_share_of_enrolled_at_month_grade_status,
        ROUND(AVG(score) OVER (PARTITION BY course_id ORDER BY enrolled_at ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS score_avg_6_prior
    FROM with_details_course
    WINDOW w_enrolled_at_month_grade_stat AS (PARTITION BY DATE_TRUNC('MONTH', enrolled_at), grade_status), w_all_by_enrolled_at AS (ORDER BY enrolled_at, id)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_grade_status_student_id_by_s, 3) AS score_avg_6_prior_cume_dist,
        ROUND(max_score_so_far / NULLIF(SUM(max_score_so_far) OVER w_distinct_term_in_enrolled_at, 0), 4) AS max_score_so_far_share_of_distinct_term_in_enrol,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY grade_status, student_id ORDER BY score_avg_6_prior DESC, id))) / 60, 1) AS minutes_since_prev_enrolled_at
    FROM windowed
    WINDOW w_grade_status_student_id_by_s AS (PARTITION BY grade_status, student_id ORDER BY score_avg_6_prior DESC, id), w_distinct_term_in_enrolled_at AS (PARTITION BY distinct_term_in_enrolled_at_month_grade_status, student_id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    course_code,
    second_enrolled_at,
    score_avg_7,
    prev2_score,
    rows_in_enrolled_at_month_grade_status,
    next_score,
    max_score_so_far,
    enrolled_at_month_grade_status_max_score,
    next3_score,
    score_share_of_enrolled_at_month_grade_status,
    score_avg_6_prior,
    score_avg_6_prior_cume_dist,
    minutes_since_prev_enrolled_at,
    LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug
FROM with_trends
WHERE next_score < 57.6
ORDER BY id NULLS LAST
