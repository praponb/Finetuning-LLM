WITH recent AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        SUBSTRING(TRIM(grade_status), 1, 3) AS grade_status_prefix
    FROM enrollments
    WHERE course_id >= 3
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_prefix,
        st.student_name,
        st.is_international AS student_is_international
    FROM recent s
    JOIN students st ON st.id = s.student_id
    WHERE st.is_international = TRUE
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(enrolled_at, 3) OVER w_course_id_student_is_interna AS prev3_enrolled_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_id ORDER BY score), 3) AS score_percent_rank,
        ROUND(score / NULLIF(SUM(score) OVER (PARTITION BY course_id), 0), 4) AS score_share_of_course_id,
        RANK() OVER (PARTITION BY course_id, student_is_international ORDER BY score) AS score_rank,
        RANK() OVER (ORDER BY score DESC NULLS FIRST, id) AS score_rank_2,
        LEAD(enrolled_at) OVER w_course_id_by_score AS next_enrolled_at,
        COUNT(*) OVER w_student_is_international AS rows_in_student_is_international,
        COUNT(*) OVER (PARTITION BY course_id) AS rows_in_course_id,
        LAG(score, 2, 0) OVER (PARTITION BY student_is_international, course_id ORDER BY enrolled_at ASC, id) AS prev2_score,
        ROW_NUMBER() OVER w_all_by_score AS rn
    FROM with_parent
    WINDOW w_course_id_student_is_interna AS (PARTITION BY course_id, student_is_international ORDER BY score, id), w_course_id_by_score AS (PARTITION BY course_id ORDER BY score, id), w_student_is_international AS (PARTITION BY student_is_international), w_all_by_score AS (ORDER BY score DESC NULLS FIRST, id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        grade_status_prefix,
        student_name,
        student_is_international,
        score_percent_rank,
        score_share_of_course_id,
        score_rank_2,
        next_enrolled_at,
        rows_in_student_is_international,
        prev2_score,
        CASE WHEN grade_status IN ('FAIL', 'INCOMPLETE') THEN 'group_a' WHEN grade_status = 'PASS' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        next_enrolled_at + INTERVAL 2 HOURS AS next_enrolled_at_plus_2h
    FROM with_trends
    WHERE rn BETWEEN 2 AND 9
)
SELECT
    id,
    course_id,
    score,
    grade_status,
    enrolled_at,
    grade_status_prefix,
    student_name,
    score_percent_rank,
    score_share_of_course_id,
    rows_in_student_is_international,
    prev2_score,
    next_enrolled_at_plus_2h
FROM labeled
ORDER BY grade_status_prefix DESC, course_id DESC
