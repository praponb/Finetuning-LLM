WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('WITHDRAWN', 'PASS') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE score > 11.3
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        term,
        grade_status,
        enrolled_at,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        COALESCE(score, 0.0) AS score_filled,
        QUARTER(enrolled_at) AS enrolled_at_quarter
    FROM valid_enrollments
),
combined_details AS (
    -- Join students
    SELECT
        s.*,
        st.cohort AS student_cohort,
        st.is_international AS student_is_international,
        st.major AS student_major
    FROM enriched s
    JOIN students st ON st.id = s.student_id AND st.student_name IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(score_filled, 3) OVER (ORDER BY enrolled_at_quarter DESC, id) AS next3_score_filled,
        LEAD(grade_status_len, 1, 0) OVER (ORDER BY enrolled_at_quarter DESC, id) AS next_grade_status_len,
        SIZE(COLLECT_SET(student_major) OVER (PARTITION BY grade_status)) AS distinct_student_major_in_grade_status,
        FIRST_VALUE(score_filled) OVER (ORDER BY enrolled_at_quarter DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_score_filled,
        NTH_VALUE(score_filled, 2) OVER (ORDER BY score_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_score_filled,
        ROUND(enrolled_at_quarter / NULLIF(SUM(enrolled_at_quarter) OVER (PARTITION BY grade_status), 0), 4) AS enrolled_at_quarter_share_of_grade_status
    FROM combined_details
),
combined_details_2 AS (
    -- Attach student attributes
    SELECT
        s.id,
        s.student_id,
        s.term,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_len,
        s.score_filled,
        s.student_cohort,
        s.student_is_international,
        s.next_grade_status_len,
        s.first_score_filled,
        s.enrolled_at_quarter_share_of_grade_status,
        st.cohort AS student_cohort_2,
        st.major AS student_major_2,
        st.gpa AS student_gpa
    FROM sequenced s
    LEFT JOIN students st ON st.id = s.student_id AND st.is_international = TRUE
)
SELECT id, measure, value
FROM (
    SELECT id, next_grade_status_len, grade_status_len
    FROM combined_details_2
) src
UNPIVOT (
    value FOR measure IN (next_grade_status_len, grade_status_len)
) unp
