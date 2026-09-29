WITH base AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 57.6 THEN 'large' WHEN score >= 36.6 THEN 'medium' ELSE 'small' END AS score_tier
    FROM enrollments
    WHERE HOUR(enrolled_at) BETWEEN 9 AND 12
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        course_id,
        term,
        score_tier,
        RANK() OVER w_term_by_enrolled_at_ties AS enrolled_at_rank,
        ROW_NUMBER() OVER (PARTITION BY term ORDER BY enrolled_at, id) AS seq_no,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id, term ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY enrolled_at DESC, id), 3) AS enrolled_at_percent_rank,
        NTILE(10) OVER (ORDER BY enrolled_at DESC, id) AS enrolled_at_decile
    FROM base
    WINDOW w_term_by_enrolled_at_ties AS (PARTITION BY term ORDER BY enrolled_at)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        term,
        enrolled_at_rank,
        first_known_enrolled_at,
        enrolled_at_percent_rank,
        enrolled_at_decile,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY course_id)) AS distinct_student_id_in_course_id,
        enrolled_at_percent_rank - LAG(enrolled_at_percent_rank) OVER (ORDER BY enrolled_at_decile DESC, id) AS enrolled_at_percent_rank_change,
        LEAD(seq_no) OVER (PARTITION BY term, enrolled_at_decile ORDER BY first_known_enrolled_at DESC, id) AS next_seq_no,
        ROUND(CUME_DIST() OVER w_all_by_enrolled_at_decile, 3) AS enrolled_at_decile_cume_dist,
        SIZE(COLLECT_SET(score_tier) OVER w_course_id) AS distinct_score_tier_in_course_id
    FROM windowed
    WINDOW w_all_by_enrolled_at_decile AS (ORDER BY enrolled_at_decile DESC, id), w_course_id AS (PARTITION BY course_id)
)
SELECT
    id,
    term,
    enrolled_at_rank,
    first_known_enrolled_at,
    enrolled_at_percent_rank,
    enrolled_at_decile,
    enrolled_at_percent_rank_change,
    next_seq_no,
    enrolled_at_decile_cume_dist,
    distinct_score_tier_in_course_id,
    DATEDIFF(DATE '2025-12-31', first_known_enrolled_at) AS days_since_first_known_enrolled_at
FROM with_trends
ORDER BY id DESC
