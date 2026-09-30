WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at
    FROM enrollments
    WHERE TO_DATE(enrolled_at) < DATE '2025-03-08'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(days_since_enrolled_at, 2) OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_enrolled_at,
        RANK() OVER w_student_id_by_enrolled_at_ti AS enrolled_at_rank,
        COUNT(*) OVER (PARTITION BY student_id) AS rows_in_student_id,
        LAST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY student_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_enrolled_at
    FROM recent
    WINDOW w_student_id_by_enrolled_at_ti AS (PARTITION BY student_id ORDER BY enrolled_at)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_id,
        term,
        score,
        enrolled_at,
        enrolled_at_rank,
        rows_in_student_id,
        RANK() OVER (PARTITION BY student_id ORDER BY score DESC NULLS FIRST) AS score_rank,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        ROUND(CUME_DIST() OVER w_student_id_by_score, 3) AS score_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY score DESC NULLS FIRST, id) AS score_seq,
        ROUND(days_since_enrolled_at / NULLIF(SUM(days_since_enrolled_at) OVER w_course_id, 0), 4) AS days_since_enrolled_at_share_of_course_id
    FROM metrics
    WINDOW w_student_id_by_score AS (PARTITION BY student_id ORDER BY score DESC NULLS FIRST, id), w_course_id AS (PARTITION BY course_id)
),
unique_rows AS (
    -- Keep the latest row per term, course_id
    SELECT
        id,
        course_id,
        term,
        score,
        enrolled_at,
        enrolled_at_rank,
        rows_in_student_id,
        score_rank,
        rows_in_course_id,
        score_cume_dist,
        score_seq,
        days_since_enrolled_at_share_of_course_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY term, course_id ORDER BY enrolled_at DESC, id DESC) AS rn
        FROM sequenced
    ) ranked
    WHERE rn = 1
)
SELECT
    enrolled_at_rank,
    term,
    COUNT(*) AS n_rows
FROM unique_rows
GROUP BY CUBE(enrolled_at_rank, term)
