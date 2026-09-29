WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.student_id,
        a.enrolled_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.score) AS prior_score_12h
    FROM enrollments a
    LEFT JOIN enrollments b
      ON b.student_id = a.student_id
     AND b.enrolled_at < a.enrolled_at
     AND b.enrolled_at >= a.enrolled_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.student_id, a.enrolled_at
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(prior_rows_12h) OVER (PARTITION BY YEAR(enrolled_at), prior_rows_12h ORDER BY prior_rows_12h DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_prior_rows_12h_so_far,
        FIRST_VALUE(prior_score_12h) OVER (PARTITION BY YEAR(enrolled_at), prior_rows_12h ORDER BY prior_rows_12h DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prior_score_12h,
        SUM(prior_rows_12h) OVER (PARTITION BY prior_rows_12h ORDER BY prior_rows_12h DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS prior_rows_12h_sum_7,
        LEAD(enrolled_at) OVER w_enrolled_at_year_prior_rows_3 AS next_enrolled_at,
        ROUND(AVG(prior_rows_12h) OVER (PARTITION BY YEAR(enrolled_at), prior_rows_12h ORDER BY prior_rows_12h DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS prior_rows_12h_avg_7,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_enrolled_at_year_prior_rows_3)) / 60, 1) AS minutes_since_prev_enrolled_at
    FROM covered
    WINDOW w_enrolled_at_year_prior_rows_3 AS (PARTITION BY YEAR(enrolled_at), prior_rows_12h ORDER BY prior_rows_12h DESC, id)
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN e.grade_status NOT IN ('PASS', 'INCOMPLETE') THEN 1 END) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_grade_status_count,
        (SELECT MAX(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS max_enrollment_score,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM with_trends s
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(prior_rows_12h_sum_7) OVER (PARTITION BY student_id ORDER BY minutes_since_prev_enrolled_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS prior_rows_12h_sum_7_avg_3_prior,
        ROUND(prior_rows_12h_avg_7 - AVG(prior_rows_12h_avg_7) OVER (PARTITION BY TO_DATE(next_enrolled_at), MONTH(enrolled_at)), 2) AS prior_rows_12h_avg_7_vs_next_enrolled_at_to_date,
        ROUND(prior_rows_12h / NULLIF(SUM(prior_rows_12h) OVER w_next_enrolled_at_to_date_enr, 0), 4) AS prior_rows_12h_share_of_next_enrolled_at_to_date,
        ROUND(CUME_DIST() OVER w_enrollment_grade_status_coun, 3) AS minutes_since_prev_enrolled_at_cume_dist,
        LAG(prior_rows_12h) OVER w_student_id_by_minutes_since_2 AS prev_prior_rows_12h,
        SUM(prior_rows_12h) OVER (PARTITION BY TO_DATE(next_enrolled_at), MONTH(enrolled_at)) AS next_enrolled_at_to_date_enrolled_at_month_sum_p
    FROM summarized_rows
    WINDOW w_next_enrolled_at_to_date_enr AS (PARTITION BY TO_DATE(next_enrolled_at), MONTH(enrolled_at)), w_enrollment_grade_status_coun AS (PARTITION BY enrollment_grade_status_count, prior_rows_12h ORDER BY minutes_since_prev_enrolled_at ASC), w_student_id_by_minutes_since_2 AS (PARTITION BY student_id ORDER BY minutes_since_prev_enrolled_at DESC, id)
)
-- Subtotals with CUBE
SELECT
    enrollment_count,
    enrollment_grade_status_count,
    ROUND(AVG(first_prior_score_12h), 2) AS avg_first_prior_score_12h,
    MAX(prior_rows_12h_avg_7) AS max_prior_rows_12h_avg_7
FROM ranked
GROUP BY CUBE(enrollment_count, enrollment_grade_status_count)
ORDER BY enrollment_count NULLS LAST, enrollment_grade_status_count NULLS LAST
