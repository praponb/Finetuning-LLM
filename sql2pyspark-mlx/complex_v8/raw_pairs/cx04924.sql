WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2026-01-01', enrolled_at) AS days_since_enrolled_at,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref
    FROM enrollments
    WHERE grade_status = 'FAIL'
      AND score < 78.8
),
with_parent_course AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.days_since_enrolled_at,
        s.grade_status_ref,
        c.credits AS course_credits,
        c.course_code,
        c.level AS course_level
    FROM valid s
    JOIN courses c ON c.id = s.course_id
    WHERE UPPER(TRIM(c.course_code)) LIKE 'C%'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY course_level ORDER BY enrolled_at), 3) AS enrolled_at_cume_dist,
        FIRST_VALUE(score) OVER (PARTITION BY course_level ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score,
        NTILE(10) OVER w_course_credits_student_id_by AS days_since_enrolled_at_decile,
        LAG(course_credits) OVER w_term_course_credits_by_score AS prev_course_credits,
        ROW_NUMBER() OVER (PARTITION BY course_credits, student_id ORDER BY days_since_enrolled_at, id) AS rn,
        DENSE_RANK() OVER (PARTITION BY course_credits, student_id ORDER BY days_since_enrolled_at) AS days_since_enrolled_at_dense_rank,
        ROUND(days_since_enrolled_at / NULLIF(SUM(days_since_enrolled_at) OVER w_student_id_course_id, 0), 4) AS days_since_enrolled_at_share_of_student_id_cours,
        FIRST_VALUE(score) OVER (ORDER BY score ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score_2,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id, course_id), 2) AS score_vs_student_id_course_id_avg
    FROM with_parent_course
    WINDOW w_course_credits_student_id_by AS (PARTITION BY course_credits, student_id ORDER BY days_since_enrolled_at, id), w_term_course_credits_by_score AS (PARTITION BY term, course_credits ORDER BY score, id), w_student_id_course_id AS (PARTITION BY student_id, course_id)
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM sequenced
    WHERE days_since_enrolled_at_dense_rank <= 3
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(prev_course_credits) OVER (PARTITION BY course_credits, student_id) AS course_credits_student_id_min_prev_course_credit,
        ROUND(score / NULLIF(SUM(score) OVER w_course_credits_student_id, 0), 4) AS score_share_of_course_credits_student_id,
        LEAD(rn, 3) OVER (PARTITION BY days_since_enrolled_at_decile ORDER BY rn DESC, id) AS next3_rn,
        ROUND(AVG(first_score_2) OVER (PARTITION BY prev_course_credits ORDER BY first_score DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS first_score_2_avg_2_prior
    FROM leaders
    WINDOW w_course_credits_student_id AS (PARTITION BY course_credits, student_id)
)
SELECT
    id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    days_since_enrolled_at,
    course_credits,
    course_code,
    course_level,
    enrolled_at_cume_dist,
    first_score,
    days_since_enrolled_at_decile,
    rn,
    days_since_enrolled_at_dense_rank,
    first_score_2,
    score_vs_student_id_course_id_avg,
    score_share_of_course_credits_student_id,
    next3_rn,
    first_score_2_avg_2_prior
FROM metrics
