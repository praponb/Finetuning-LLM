WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
),
enriched_student AS (
    -- Enrich with dimension data
    SELECT s.*, st.cohort AS student_cohort
    FROM scoped s
    JOIN students st ON st.id = s.student_id
    WHERE st.cohort = 'C2022'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        enrolled_at_date,
        ROW_NUMBER() OVER w_all_by_enrolled_at AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at NULLS LAST, id) AS rn,
        DENSE_RANK() OVER w_all_by_enrolled_at_2 AS enrolled_at_dense_rank,
        RANK() OVER (ORDER BY enrolled_at ASC NULLS LAST, id) AS enrolled_at_rank,
        RANK() OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at NULLS LAST) AS enrolled_at_rank_2,
        DENSE_RANK() OVER (ORDER BY enrolled_at DESC NULLS FIRST) AS enrolled_at_dense_rank_2,
        LEAD(enrolled_at, 3) OVER (PARTITION BY student_id, grade_status ORDER BY enrolled_at NULLS LAST, id) AS next3_enrolled_at,
        LEAD(enrolled_at, 2) OVER (ORDER BY enrolled_at DESC NULLS FIRST, id) AS next2_enrolled_at,
        RANK() OVER w_all_by_enrolled_at_ties_2 AS enrolled_at_rank_3
    FROM enriched_student
    WINDOW w_all_by_enrolled_at AS (ORDER BY enrolled_at ASC NULLS LAST, id), w_all_by_enrolled_at_2 AS (ORDER BY enrolled_at DESC, id), w_all_by_enrolled_at_ties_2 AS (ORDER BY enrolled_at ASC NULLS LAST)
)
-- Final projection
SELECT
    id,
    student_id,
    term,
    grade_status,
    enrolled_at,
    enrolled_at_date,
    rn,
    enrolled_at_rank,
    enrolled_at_rank_2,
    enrolled_at_dense_rank_2,
    next3_enrolled_at,
    next2_enrolled_at
FROM with_windows
ORDER BY rn DESC
