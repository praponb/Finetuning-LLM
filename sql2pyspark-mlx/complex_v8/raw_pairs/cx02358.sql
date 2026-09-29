WITH valid AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year,
        ABS(score - 69.6) AS score_distance
    FROM enrollments
    WHERE grade_status <> 'PASS'
),
joined_student AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.enrolled_at,
        st.enrolled_on AS student_enrolled_on,
        st.student_name,
        st.major AS student_major
    FROM valid s
    LEFT JOIN students st ON st.id = s.student_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY student_id)) AS distinct_course_id_in_student_id,
        FIRST_VALUE(enrolled_at) OVER (ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_enrolled_at,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY MONTH(student_enrolled_on), student_major ORDER BY enrolled_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROW_NUMBER() OVER (PARTITION BY course_id, DATE_TRUNC('MONTH', enrolled_at) ORDER BY student_enrolled_on DESC, id) AS rn,
        NTH_VALUE(student_enrolled_on, 2) OVER (ORDER BY student_enrolled_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_student_enrolled_on,
        DENSE_RANK() OVER (PARTITION BY course_id, DATE_TRUNC('MONTH', enrolled_at) ORDER BY student_enrolled_on DESC, id) AS student_enrolled_on_dense_rank,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY student_id), 0), 4) AS score_share_of_student_id,
        SIZE(COLLECT_SET(course_id) OVER (PARTITION BY MONTH(enrolled_at), student_major)) AS distinct_course_id_in_enrolled_at_month_student_,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY MONTH(enrolled_at), student_major)) AS distinct_student_id_in_enrolled_at_month_student,
        MIN(score) OVER (PARTITION BY MONTH(enrolled_at), student_major) AS enrolled_at_month_student_major_min_score,
        FIRST_VALUE(student_enrolled_on) IGNORE NULLS OVER (ORDER BY student_enrolled_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_student_enrolled_on,
        RANK() OVER (ORDER BY student_enrolled_on ASC NULLS LAST, id) AS student_enrolled_on_rank,
        NTH_VALUE(score, 2) OVER (PARTITION BY course_id, student_major ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score,
        MAX(score) OVER (PARTITION BY course_id, DATE_TRUNC('MONTH', enrolled_at) ORDER BY student_enrolled_on DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS score_max_6_prior
    FROM joined_student
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM windowed
    WHERE rn <= 3
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(second_enrolled_at) AS second_enrolled_at_year,
        student_enrolled_on_rank,
        distinct_student_id_in_enrolled_at_month_student,
        MIN(rn) AS min_rn,
        MAX(score_max_6_prior) AS max_score_max_6_prior,
        COUNT(*) AS n_rows
    FROM leaders
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    second_enrolled_at_year,
    student_enrolled_on_rank,
    min_rn,
    max_score_max_6_prior,
    n_rows
FROM rollup_base
ORDER BY second_enrolled_at_year DESC
