WITH scoped_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 69.6 THEN 'A' WHEN score >= 13.9 THEN 'B' ELSE 'C' END AS score_tier
    FROM enrollments
    WHERE (student_id IS NOT NULL OR TO_DATE(enrolled_at) < DATE '2025-02-06')
),
with_parent AS (
    -- Attach student attributes
    SELECT
        s.*,
        st.is_international AS student_is_international,
        st.major AS student_major
    FROM scoped_enrollments s
    JOIN students st ON st.id = s.student_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        score_tier,
        student_is_international,
        student_major,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY student_major, score_tier ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        ROUND(CUME_DIST() OVER (PARTITION BY term, student_is_international ORDER BY enrolled_at DESC), 3) AS enrolled_at_cume_dist,
        LEAD(enrolled_at) OVER (ORDER BY enrolled_at DESC, id) AS next_enrolled_at,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_all_by_enrolled_at)) / 60, 1) AS minutes_since_prev_enrolled_at,
        NTILE(10) OVER (ORDER BY enrolled_at DESC, id) AS enrolled_at_decile,
        DENSE_RANK() OVER (PARTITION BY grade_status ORDER BY enrolled_at) AS enrolled_at_dense_rank,
        ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(LAG(enrolled_at) OVER w_student_major_score_tier_by_2)) / 60, 1) AS minutes_since_prev_enrolled_at_2,
        LEAD(enrolled_at, 2) OVER w_all_by_enrolled_at AS next2_enrolled_at,
        LEAD(enrolled_at, 3) OVER w_grade_status_by_enrolled_at_2 AS next3_enrolled_at
    FROM with_parent
    WINDOW w_all_by_enrolled_at AS (ORDER BY enrolled_at DESC, id), w_student_major_score_tier_by_2 AS (PARTITION BY student_major, score_tier ORDER BY enrolled_at DESC, id), w_grade_status_by_enrolled_at_2 AS (PARTITION BY grade_status ORDER BY enrolled_at, id)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', next3_enrolled_at) AS days_since_next3_enrolled_at
    FROM sequenced
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY enrolled_at_dense_rank, enrolled_at_decile) AS rows_in_enrolled_at_dense_rank_enrolled_at_decil,
        ROUND(days_since_next3_enrolled_at / NULLIF(SUM(days_since_next3_enrolled_at) OVER (PARTITION BY enrolled_at_dense_rank, enrolled_at_decile), 0), 4) AS days_since_next3_enrolled_at_share_of_enrolled_a,
        LAG(second_enrolled_at) OVER (PARTITION BY term, score_tier ORDER BY enrolled_at_decile DESC, id) AS prev_second_enrolled_at,
        RANK() OVER (PARTITION BY YEAR(next2_enrolled_at) ORDER BY next3_enrolled_at, id) AS next3_enrolled_at_rank
    FROM labeled
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    score_tier,
    student_is_international,
    student_major,
    next_enrolled_at,
    second_enrolled_at,
    minutes_since_prev_enrolled_at,
    enrolled_at_dense_rank,
    minutes_since_prev_enrolled_at_2,
    next2_enrolled_at,
    next3_enrolled_at,
    days_since_next3_enrolled_at,
    rows_in_enrolled_at_dense_rank_enrolled_at_decil,
    prev_second_enrolled_at,
    SUBSTRING(TRIM(score_tier), 1, 3) AS score_tier_prefix
FROM sequenced_2
WHERE student_major NOT IN ('CS', 'Biz')
ORDER BY days_since_next3_enrolled_at, rows_in_enrolled_at_dense_rank_enrolled_at_decil
