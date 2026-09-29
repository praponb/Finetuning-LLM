WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug
    FROM enrollments
    WHERE student_id >= 19
),
combined_details_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.score_filled,
        s.grade_status_slug,
        c.course_code
    FROM recent s
    JOIN courses c ON c.id = s.course_id
    WHERE UPPER(TRIM(c.course_code)) LIKE 'C%'
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        enrolled_at + INTERVAL 7 DAYS AS enrolled_at_plus_7d,
        DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref
    FROM combined_details_course
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_id,
        score,
        grade_status,
        enrolled_at,
        score_filled,
        grade_status_slug,
        enrolled_at_year,
        term_ref,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(enrolled_at_plus_7d), student_id ORDER BY score ASC, id), 3) AS score_cume_dist,
        COUNT(*) OVER (PARTITION BY student_id) AS rows_in_student_id,
        MIN(score) OVER (PARTITION BY student_id) AS student_id_min_score,
        ROUND(score / NULLIF(SUM(score) OVER w_student_id, 0), 4) AS score_share_of_student_id,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id), 2) AS score_vs_student_id_avg,
        DENSE_RANK() OVER w_enrolled_at_plus_7d_month_st_2 AS score_dense_rank
    FROM derived
    WINDOW w_student_id AS (PARTITION BY student_id), w_enrolled_at_plus_7d_month_st_2 AS (PARTITION BY MONTH(enrolled_at_plus_7d), student_id ORDER BY score ASC)
)
-- Final projection
SELECT
    id,
    student_id,
    score,
    grade_status,
    enrolled_at,
    grade_status_slug,
    enrolled_at_year,
    term_ref,
    score_cume_dist,
    student_id_min_score,
    score_share_of_student_id,
    score_vs_student_id_avg,
    score_dense_rank,
    COALESCE(score_dense_rank, 0) AS score_dense_rank_filled
FROM windowed
ORDER BY enrolled_at DESC
