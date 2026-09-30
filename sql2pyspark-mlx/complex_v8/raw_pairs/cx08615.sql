WITH cleaned AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        LOWER(REGEXP_REPLACE(TRIM(grade_status), ' ', '_')) AS grade_status_slug,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown
    FROM enrollments
    WHERE (course_id >= 11 OR student_id >= 18)
),
combined_details_student AS (
    -- Join students
    SELECT
        s.*,
        st.gpa AS student_gpa,
        st.cohort AS student_cohort,
        st.enrolled_on AS student_enrolled_on
    FROM cleaned s
    JOIN students st ON st.id = s.student_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        score,
        grade_status,
        enrolled_at,
        score_filled,
        term_or_unknown,
        student_cohort,
        student_enrolled_on,
        SUM(score_filled) OVER (PARTITION BY grade_status, term_or_unknown) AS grade_status_term_or_unknown_sum_score_filled,
        FIRST_VALUE(score_filled) IGNORE NULLS OVER (PARTITION BY grade_status ORDER BY score_filled ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_score_filled,
        ROUND(PERCENT_RANK() OVER w_term_or_unknown_by_score_fil, 3) AS score_filled_percent_rank
    FROM combined_details_student
    WINDOW w_term_or_unknown_by_score_fil AS (PARTITION BY term_or_unknown ORDER BY score_filled DESC)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        enrolled_at,
        score_filled,
        term_or_unknown,
        student_cohort,
        student_enrolled_on,
        grade_status_term_or_unknown_sum_score_filled,
        first_known_score_filled,
        QUARTER(enrolled_at) AS enrolled_at_quarter,
        ROUND(first_known_score_filled * 1.07, 2) AS first_known_score_filled_adj,
        COALESCE(term_or_unknown, 'UNKNOWN') AS term_or_unknown_or_unknown
    FROM ranked
    WHERE student_id <= 9
)
SELECT
    id,
    score_filled,
    term_or_unknown,
    student_cohort,
    student_enrolled_on,
    grade_status_term_or_unknown_sum_score_filled,
    first_known_score_filled,
    enrolled_at_quarter,
    first_known_score_filled_adj,
    term_or_unknown_or_unknown,
    SUBSTRING(TRIM(term_or_unknown), 1, 2) AS term_or_unknown_prefix,
    CASE WHEN first_known_score_filled >= 80.0 THEN 'A' WHEN first_known_score_filled >= 30.0 THEN 'B' ELSE 'C' END AS first_known_score_filled_tier
FROM tagged
ORDER BY first_known_score_filled_tier DESC
