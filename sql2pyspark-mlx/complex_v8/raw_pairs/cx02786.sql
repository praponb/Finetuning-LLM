WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM enrollments
    WHERE score >= 38.3
      AND student_id IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(score / NULLIF(SUM(score) OVER w_student_id_course_id, 0), 4) AS score_share_of_student_id_course_id,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id), 0), 4) AS score_share_of_student_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY score DESC), 3) AS score_percent_rank,
        LEAD(score) OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY score DESC, id) AS next_score,
        MAX(score) OVER (ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_score_so_far,
        LAG(score) OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY score DESC, id) AS prev_score,
        MIN(score) OVER w_student_id_course_id AS student_id_course_id_min_score,
        COUNT(*) OVER w_student_id AS rows_in_student_id,
        ROUND(CUME_DIST() OVER w_enrolled_at_to_date_by_score_2, 3) AS score_cume_dist,
        ROUND(AVG(score) OVER (PARTITION BY student_id), 2) AS student_id_avg_score,
        NTILE(5) OVER (PARTITION BY student_id ORDER BY enrolled_at, id) AS enrolled_at_quintile,
        ROUND(AVG(score) OVER (PARTITION BY student_id, course_id ORDER BY score, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS score_avg_7,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY student_id ORDER BY enrolled_at, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        SUM(score) OVER (PARTITION BY TO_DATE(enrolled_at) ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_score,
        ROUND(AVG(score) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS score_avg_1_prior,
        COUNT(*) OVER (PARTITION BY student_id, course_id) AS rows_in_student_id_course_id
    FROM valid
    WINDOW w_student_id_course_id AS (PARTITION BY student_id, course_id), w_student_id AS (PARTITION BY student_id), w_enrolled_at_to_date_by_score_2 AS (PARTITION BY TO_DATE(enrolled_at) ORDER BY score DESC, id)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(TRIM(grade_status_slug)) AS grade_status_slug_clean,
        CONCAT(grade_status_slug, '#', CAST(student_id AS STRING)) AS grade_status_slug_ref,
        COALESCE(prev_score, 0.0) AS prev_score_filled
    FROM with_trends
    WHERE next_score > 10.8
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY student_id ORDER BY enrolled_at_quintile, id) AS enrolled_at_quintile_rank,
        SUM(score_percent_rank) OVER (PARTITION BY course_id ORDER BY student_id_avg_score, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS score_percent_rank_sum_3,
        DENSE_RANK() OVER (PARTITION BY student_id ORDER BY enrolled_at_quintile) AS enrolled_at_quintile_dense_rank,
        SUM(prev_score_filled) OVER (PARTITION BY student_id ORDER BY enrolled_at_quintile, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS prev_score_filled_sum_6_prior,
        NTH_VALUE(prev_score_filled, 2) OVER (PARTITION BY course_id ORDER BY student_id_avg_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev_score_filled,
        RANK() OVER (PARTITION BY student_id ORDER BY enrolled_at_quintile) AS enrolled_at_quintile_rank_2
    FROM scored
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    score,
    enrolled_at,
    grade_status_slug,
    score_share_of_student_id_course_id,
    score_percent_rank,
    next_score,
    max_score_so_far,
    prev_score,
    student_id_course_id_min_score,
    rows_in_student_id,
    score_cume_dist,
    student_id_avg_score,
    score_avg_7,
    minutes_since_prev_enrolled_at,
    running_score,
    grade_status_slug_clean,
    prev_score_filled,
    score_percent_rank_sum_3,
    second_prev_score_filled,
    enrolled_at_quintile_rank_2
FROM metrics
