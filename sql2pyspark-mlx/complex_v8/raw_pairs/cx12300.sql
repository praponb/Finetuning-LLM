WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE course_id IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at))) AS distinct_course_id_in_enrolled_at_month,
        LAST_VALUE(score) IGNORE NULLS OVER (PARTITION BY course_id, term ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER (PARTITION BY term, grade_status ORDER BY enrolled_at NULLS LAST, id))) / 60, 1) AS minutes_since_prev_enrolled_at,
        DENSE_RANK() OVER (PARTITION BY term, grade_status ORDER BY enrolled_at) AS enrolled_at_dense_rank,
        SIZE(COLLECT_SET(grade_status) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at))) AS distinct_grade_status_in_enrolled_at_month,
        ROUND(score / NULLIF(SUM(score) OVER w_grade_status_course_id, 0), 4) AS score_share_of_grade_status_course_id,
        FIRST_VALUE(enrolled_at) OVER (PARTITION BY term, grade_status ORDER BY enrolled_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        ROUND(score - AVG(score) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at)), 2) AS score_vs_enrolled_at_month_avg,
        LEAD(enrolled_at) OVER (PARTITION BY course_id, term ORDER BY enrolled_at NULLS LAST, id) AS next_enrolled_at,
        ROUND(AVG(score) OVER (PARTITION BY grade_status, course_id), 2) AS grade_status_course_id_avg_score,
        MAX(score) OVER (PARTITION BY term, grade_status ORDER BY enrolled_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS score_max_7,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_at)), 0), 4) AS score_share_of_enrolled_at_month,
        DENSE_RANK() OVER (PARTITION BY term, grade_status ORDER BY enrolled_at NULLS LAST) AS enrolled_at_dense_rank_2
    FROM scoped
    WINDOW w_grade_status_course_id AS (PARTITION BY grade_status, course_id)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        distinct_course_id_in_enrolled_at_month,
        last_known_score,
        minutes_since_prev_enrolled_at,
        first_enrolled_at,
        score_vs_enrolled_at_month_avg,
        next_enrolled_at,
        grade_status_course_id_avg_score,
        score_max_7,
        score_share_of_enrolled_at_month,
        enrolled_at_dense_rank_2,
        FLOOR(score / 25) * 25 AS score_bucket,
        COALESCE(score_max_7, 0.0) AS score_max_7_filled
    FROM sequenced
    WHERE enrolled_at < TIMESTAMP '2025-02-18 11:00:00'
),
totals AS (
    -- Summarise per group
    SELECT
        CASE WHEN last_known_score >= 78.8 THEN 'HIGH' WHEN last_known_score >= 32.9 THEN 'MEDIUM' ELSE 'LOW' END AS last_known_score_tier,
        term,
        course_id,
        MIN(enrolled_at) AS first_enrolled_at_2,
        COUNT(*) FILTER (WHERE grade_status NOT IN ('INCOMPLETE', 'FAIL')) AS n_grade_status_not_in_incomple
    FROM tagged
    GROUP BY CASE WHEN last_known_score >= 78.8 THEN 'HIGH' WHEN last_known_score >= 32.9 THEN 'MEDIUM' ELSE 'LOW' END, term, course_id
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        last_known_score_tier,
        term,
        course_id,
        first_enrolled_at_2,
        n_grade_status_not_in_incomple,
        ROUND(PERCENT_RANK() OVER w_last_known_score_tier_by_n_g, 3) AS n_grade_status_not_in_incomple_percent_rank,
        ROUND((UNIX_TIMESTAMP(first_enrolled_at_2) - UNIX_TIMESTAMP(LAG(first_enrolled_at_2) OVER (PARTITION BY last_known_score_tier ORDER BY n_grade_status_not_in_incomple, last_known_score_tier, term, course_id))) / 60, 1) AS minutes_since_prev_first_enrolled_at_2,
        LAG(first_enrolled_at_2, 3) OVER (PARTITION BY last_known_score_tier ORDER BY n_grade_status_not_in_incomple, last_known_score_tier, term, course_id) AS prev3_first_enrolled_at_2,
        DENSE_RANK() OVER w_last_known_score_tier_by_n_g AS n_grade_status_not_in_incomple_dense_rank
    FROM totals
    WINDOW w_last_known_score_tier_by_n_g AS (PARTITION BY last_known_score_tier ORDER BY n_grade_status_not_in_incomple)
)
SELECT
    last_known_score_tier,
    term,
    course_id,
    first_enrolled_at_2,
    n_grade_status_not_in_incomple_percent_rank,
    minutes_since_prev_first_enrolled_at_2
FROM sequenced_2
ORDER BY last_known_score_tier, course_id NULLS LAST
