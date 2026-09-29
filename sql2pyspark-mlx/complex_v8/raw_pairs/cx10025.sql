WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        DATE_TRUNC('DAY', enrolled_at) AS enrolled_at_day
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-02-06 11:00:00'
      AND course_id IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id, grade_status), 0), 4) AS score_share_of_student_id_grade_status,
        FIRST_VALUE(enrolled_at_day) OVER (PARTITION BY MONTH(enrolled_at_day), student_id ORDER BY enrolled_at_day DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at_day,
        FIRST_VALUE(score) OVER (PARTITION BY course_id, term ORDER BY score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY student_id, grade_status)) AS distinct_course_id_in_student_id_grade_status,
        MAX(score) OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_so_far,
        FIRST_VALUE(score) IGNORE NULLS OVER (ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score
    FROM prepared_enrollments
),
without_matches AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.term NOT IN ('2025T2', '2024T2')
              AND HOUR(e.enrolled_at) BETWEEN 9 AND 13
        )
      AND s.student_id IS NOT NULL
),
student_id_stats AS (
    -- Median max_score_so_far per student_id
    SELECT student_id, PERCENTILE_APPROX(max_score_so_far, 0.5) AS median_max_score_so_far
    FROM without_matches
    GROUP BY student_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_max_score_so_far,
        ROUND(s.max_score_so_far - g.median_max_score_so_far, 2) AS max_score_so_far_minus_median
    FROM without_matches s
    JOIN student_id_stats g ON g.student_id = s.student_id
    WHERE s.max_score_so_far >= g.median_max_score_so_far
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        score_share_of_student_id_grade_status,
        first_enrolled_at_day,
        distinct_course_id_in_student_id_grade_status,
        max_score_so_far_minus_median,
        ROUND(AVG(max_score_so_far) OVER (ORDER BY median_max_score_so_far, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS max_score_so_far_avg_4,
        ROUND(AVG(median_max_score_so_far) OVER (ORDER BY median_max_score_so_far, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS median_max_score_so_far_avg_3,
        RANK() OVER w_all_by_distinct_course_id_in AS distinct_course_id_in_student_id_grade_status_ra,
        FIRST_VALUE(first_score) OVER (ORDER BY distinct_course_id_in_student_id_grade_status DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_first_score
    FROM above_average
    WINDOW w_all_by_distinct_course_id_in AS (ORDER BY distinct_course_id_in_student_id_grade_status DESC, id)
),
enriched_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.first_enrolled_at_day,
        s.max_score_so_far_minus_median,
        s.max_score_so_far_avg_4,
        s.median_max_score_so_far_avg_3,
        s.distinct_course_id_in_student_id_grade_status_ra,
        s.first_first_score,
        c.credits AS course_credits,
        st.cohort AS student_cohort
    FROM with_windows s
    LEFT JOIN courses c ON c.id = s.course_id
    LEFT JOIN students st ON st.id = s.student_id
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT course_id, MIN(score) AS min_enrollment_score
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score
    FROM enriched_course s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
SELECT
    id,
    course_id,
    term,
    score,
    grade_status,
    first_enrolled_at_day,
    max_score_so_far_minus_median,
    max_score_so_far_avg_4,
    median_max_score_so_far_avg_3,
    distinct_course_id_in_student_id_grade_status_ra,
    first_first_score,
    course_credits,
    student_cohort,
    min_enrollment_score,
    COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
    CASE WHEN first_first_score >= 59.7 THEN 'large' WHEN first_first_score >= 26.2 THEN 'medium' ELSE 'small' END AS first_first_score_tier
FROM profiled
ORDER BY id DESC, grade_status_or_unknown DESC NULLS FIRST
