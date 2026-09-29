WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at,
        UPPER(TRIM(grade_status)) AS grade_status_clean
    FROM enrollments
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER (ORDER BY enrolled_at ASC, id) AS enrolled_at_quintile,
        ROUND(AVG(score) OVER (ORDER BY enrolled_at ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS score_avg_6_prior,
        LEAD(score) OVER (PARTITION BY student_id, course_id ORDER BY score DESC, id) AS next_score,
        FIRST_VALUE(score) OVER (PARTITION BY student_id, course_id ORDER BY score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        MAX(score) OVER w_enrolled_at_to_date_term AS enrolled_at_to_date_term_max_score,
        RANK() OVER (PARTITION BY student_id, course_id ORDER BY score DESC) AS score_rank,
        MIN(score) OVER (PARTITION BY TO_DATE(enrolled_at), term) AS enrolled_at_to_date_term_min_score,
        COUNT(*) OVER w_enrolled_at_to_date_term AS rows_in_enrolled_at_to_date_term
    FROM prepared_enrollments
    WINDOW w_enrolled_at_to_date_term AS (PARTITION BY TO_DATE(enrolled_at), term)
)
-- Final projection
SELECT
    id,
    enrolled_at,
    enrolled_at_quintile,
    score_avg_6_prior,
    next_score,
    first_score,
    score_rank,
    rows_in_enrolled_at_to_date_term,
    CASE WHEN term IN ('2024T2', '2025T1') THEN 'group_a' WHEN term = '2025T2' THEN 'group_b' ELSE 'other' END AS term_group,
    score + enrolled_at_quintile AS score_plus_enrolled_at_quintile
FROM with_windows
WHERE student_id IS NOT NULL
ORDER BY term_group DESC NULLS FIRST
