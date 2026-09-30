WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix,
        DATEDIFF(DATE '2025-06-30', enrolled_at) AS days_since_enrolled_at,
        CASE WHEN score >= 77.0 THEN 'large' WHEN score >= 20.2 THEN 'medium' ELSE 'small' END AS score_tier
    FROM enrollments
    WHERE MONTH(enrolled_at) IN (1, 3)
),
without_matches AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM scoped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.score BETWEEN 23.7 AND 85.8
        )
),
annotated AS (
    -- Label and score rows
    SELECT *, DATEDIFF(DATE '2025-12-31', enrolled_at) AS days_since_enrolled_at_2
    FROM without_matches
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        term,
        score,
        days_since_enrolled_at,
        score_tier,
        days_since_enrolled_at_2,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM annotated
    WHERE score <= 57.8
),
combined_details AS (
    -- Join students
    SELECT
        s.*,
        st.major AS student_major,
        st.is_international AS student_is_international
    FROM labeled s
    LEFT JOIN students st ON st.id = s.student_id AND st.major = 'Biz'
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, MIN(score) AS min_enrollment_score
    FROM enrollments
    GROUP BY student_id
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.min_enrollment_score
    FROM combined_details s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        student_id,
        term,
        score,
        score_tier,
        days_since_enrolled_at_2,
        enrolled_at_date,
        min_enrollment_score,
        FIRST_VALUE(score) IGNORE NULLS OVER (PARTITION BY score_tier, student_is_international ORDER BY enrolled_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score,
        SIZE(COLLECT_SET(score_tier) OVER (PARTITION BY student_major, YEAR(enrolled_at_date))) AS distinct_score_tier_in_student_major_enrolled_at,
        FIRST_VALUE(min_enrollment_score) OVER (PARTITION BY student_id, score_tier ORDER BY days_since_enrolled_at_2 NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_min_enrollment_score,
        LEAD(score, 2) OVER w_all_by_days_since_enrolled_a AS next2_score,
        SUM(days_since_enrolled_at) OVER (PARTITION BY student_id, score_tier ORDER BY days_since_enrolled_at_2 NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS days_since_enrolled_at_sum_2,
        SIZE(COLLECT_SET(student_id) OVER (PARTITION BY student_major, YEAR(enrolled_at_date))) AS distinct_student_id_in_student_major_enrolled_at
    FROM summarized_rows
    WINDOW w_all_by_days_since_enrolled_a AS (ORDER BY days_since_enrolled_at ASC, id)
)
SELECT
    id,
    student_id,
    score,
    score_tier,
    days_since_enrolled_at_2,
    enrolled_at_date,
    min_enrollment_score,
    first_known_score,
    distinct_score_tier_in_student_major_enrolled_at,
    first_min_enrollment_score,
    next2_score,
    days_since_enrolled_at_sum_2,
    distinct_student_id_in_student_major_enrolled_at
FROM windowed
ORDER BY first_known_score, min_enrollment_score DESC
