WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 74.9 THEN 'large' WHEN score >= 35.8 THEN 'medium' ELSE 'small' END AS score_tier,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM enrollments
    WHERE term = '2024T1'
      AND TO_DATE(enrolled_at) < DATE '2025-03-14'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY score_tier, grade_status_len)) AS distinct_student_id_in_score_tier_grade_status_l,
        LAG(score, 1, 0) OVER (PARTITION BY grade_status, score_tier ORDER BY score ASC, id) AS prev_score,
        ROUND(CUME_DIST() OVER (PARTITION BY course_id, grade_status_len ORDER BY enrolled_at NULLS LAST, id), 3) AS enrolled_at_cume_dist,
        grade_status_len - LAG(grade_status_len) OVER (PARTITION BY course_id, grade_status_len ORDER BY enrolled_at NULLS LAST, id) AS grade_status_len_change,
        RANK() OVER (PARTITION BY grade_status, score_tier ORDER BY score ASC) AS score_rank,
        MAX(score) OVER (PARTITION BY score_tier, grade_status_len) AS score_tier_grade_status_len_max_score,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_course_id_grade_status_len_b)) / 60, 1) AS minutes_since_prev_enrolled_at,
        ROUND(score - AVG(score) OVER w_score_tier_grade_status_len, 2) AS score_vs_score_tier_grade_status_len_avg,
        score - LAG(score) OVER (PARTITION BY course_id, grade_status_len ORDER BY enrolled_at NULLS LAST, id) AS score_change
    FROM scoped
    WINDOW w_course_id_grade_status_len_b AS (PARTITION BY course_id, grade_status_len ORDER BY enrolled_at NULLS LAST, id), w_score_tier_grade_status_len AS (PARTITION BY score_tier, grade_status_len)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(' / ', grade_status, score_tier) AS grade_status_score_tier_label,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 4) AS grade_status_prefix
    FROM with_windows
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        score_rank - LAG(score_rank) OVER w_distinct_student_id_in_score AS score_rank_change,
        FIRST_VALUE(score_rank) IGNORE NULLS OVER (PARTITION BY distinct_student_id_in_score_tier_grade_status_l ORDER BY score_change DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score_rank,
        enrolled_at_cume_dist - LAG(enrolled_at_cume_dist) OVER (ORDER BY score_rank, id) AS enrolled_at_cume_dist_change
    FROM tagged
    WINDOW w_distinct_student_id_in_score AS (PARTITION BY distinct_student_id_in_score_tier_grade_status_l ORDER BY score_change DESC, id)
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status,
    score_tier,
    distinct_student_id_in_score_tier_grade_status_l,
    enrolled_at_cume_dist,
    score_rank,
    minutes_since_prev_enrolled_at,
    score_change,
    grade_status_score_tier_label,
    days_since_enrolled_at,
    score_rank_change,
    first_known_score_rank,
    enrolled_at_cume_dist_change
FROM windowed
ORDER BY grade_status NULLS LAST, first_known_score_rank DESC, id
LIMIT 5
