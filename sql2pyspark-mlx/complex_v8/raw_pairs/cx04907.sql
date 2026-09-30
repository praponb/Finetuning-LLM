WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE term = '2025T1'
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        c.course_code
    FROM scoped_enrollments s
    JOIN courses c ON c.id = s.course_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER w_student_id_course_id AS rows_in_student_id_course_id,
        ROUND(score - AVG(score) OVER w_student_id, 2) AS score_vs_student_id_avg,
        ROUND(AVG(score) OVER (ORDER BY score ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS score_avg_3,
        DENSE_RANK() OVER (PARTITION BY course_id, student_id ORDER BY score DESC NULLS FIRST, id) AS score_dense_rank,
        SIZE(COLLECT_SET(course_id) OVER w_student_id) AS distinct_course_id_in_student_id,
        NTH_VALUE(score, 2) OVER (PARTITION BY course_id, student_id ORDER BY score DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_id, student_id ORDER BY score DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        LEAD(enrolled_at) OVER w_student_id_by_enrolled_at AS next_enrolled_at,
        DENSE_RANK() OVER (ORDER BY score ASC, id) AS score_dense_rank_2,
        FIRST_VALUE(score) OVER (PARTITION BY course_id, student_id ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        SUM(score) OVER (PARTITION BY course_id, student_id ORDER BY score DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS score_sum_2_prior,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        score - LAG(score) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS score_change
    FROM with_details
    WINDOW w_student_id_course_id AS (PARTITION BY student_id, course_id), w_student_id AS (PARTITION BY student_id), w_student_id_by_enrolled_at AS (PARTITION BY student_id ORDER BY enrolled_at, id)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        enrolled_at,
        rows_in_student_id_course_id,
        score_vs_student_id_avg,
        second_enrolled_at,
        rows_in_course_id,
        next_enrolled_at,
        first_score,
        score_sum_2_prior,
        minutes_since_prev_enrolled_at,
        DATE_TRUNC('YEAR', second_enrolled_at) AS second_enrolled_at_year,
        ABS(first_score - 63.9) AS first_score_distance,
        DAYOFWEEK(enrolled_at) AS enrolled_at_dayofweek
    FROM with_windows
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        enrolled_at_dayofweek,
        CASE WHEN first_score_distance >= 65.0 THEN 'large' WHEN first_score_distance >= 40.0 THEN 'medium' ELSE 'small' END AS first_score_distance_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(enrolled_at_dayofweek) AS total_enrolled_at_dayofweek,
        COUNT(DISTINCT student_id) AS distinct_student_ids,
        MAX(enrolled_at_dayofweek) AS max_enrolled_at_dayofweek,
        MIN(enrolled_at) AS first_enrolled_at,
        COUNT(*) AS cnt
    FROM enriched
    GROUP BY enrolled_at_dayofweek, CASE WHEN first_score_distance >= 65.0 THEN 'large' WHEN first_score_distance >= 40.0 THEN 'medium' ELSE 'small' END
)
SELECT
    enrolled_at_dayofweek,
    first_score_distance_tier,
    distinct_ids,
    total_enrolled_at_dayofweek,
    distinct_student_ids,
    max_enrolled_at_dayofweek,
    first_enrolled_at,
    HOUR(first_enrolled_at) AS first_enrolled_at_hour,
    GREATEST(enrolled_at_dayofweek, distinct_student_ids) AS greatest_enrolled_at_dayofweek_distinct_student_
FROM totals
WHERE first_score_distance_tier = 'large'
ORDER BY distinct_student_ids, enrolled_at_dayofweek DESC
