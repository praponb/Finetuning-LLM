WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        ROUND(score * 0.25, 2) AS score_adj
    FROM enrollments
    WHERE score >= 34.2
),
annotated AS (
    -- Derive helper columns
    SELECT *, score_adj + score AS score_adj_plus_score
    FROM cleaned
),
with_details_course AS (
    -- Join courses
    SELECT
        s.*,
        c.course_code,
        c.level AS course_level,
        c.credits AS course_credits
    FROM annotated s
    JOIN courses c ON c.id = s.course_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(course_credits, 0) AS course_credits_filled,
        LEAST(score_adj, course_credits) AS least_score_adj_course_credits,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM with_details_course
    WHERE score <= 57.0
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY course_credits_filled, term) AS rows_in_course_credits_filled_term,
        ROUND(score_adj_plus_score / NULLIF(SUM(score_adj_plus_score) OVER (PARTITION BY course_credits_filled, term), 0), 4) AS score_adj_plus_score_share_of_course_credits_fil
    FROM labeled
),
stats AS (
    -- Summarise per group
    SELECT
        course_credits,
        MIN(score_adj_plus_score) AS min_score_adj_plus_score,
        ARRAY_SORT(COLLECT_SET(course_level)) AS course_level_list,
        ROUND(SUM(rows_in_course_credits_filled_term) / NULLIF(SUM(score_adj_plus_score_share_of_course_credits_fil), 0), 3) AS rows_in_course_credits_filled_term_to_score_adj_,
        SUM(grade_status_len) AS total_grade_status_len
    FROM with_windows
    GROUP BY course_credits
    HAVING SUM(score_adj_plus_score_share_of_course_credits_fil) > 0.1
)
SELECT course_credits, min_score_adj_plus_score
FROM stats
ORDER BY min_score_adj_plus_score DESC, course_credits DESC
