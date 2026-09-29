WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN term IN ('2025T1', '2024T2') THEN 'group_a' WHEN term = '2024T1' THEN 'group_b' ELSE 'other' END AS term_group
    FROM enrollments
    WHERE score > 32.7
      AND student_id IS NOT NULL
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM prepared_enrollments
    WHERE score < (SELECT MAX(score) FROM prepared_enrollments)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        term_group,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month,
        student_id,
        ROUND(100 * COUNT(CASE WHEN term IN ('2024T2', '2024T1', '2025T1') THEN 1 END) / COUNT(*), 1) AS pct_term_in_2024t2_2024t1_2025t1,
        SUM(CASE WHEN grade_status = 'INCOMPLETE' THEN score ELSE 0 END) AS score_grade_status_incomplete,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list
    FROM filtered
    GROUP BY 1, 2, 3
    HAVING AVG(score) > 34.2
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS first_enrollment_enrolled_at
    FROM rollup_base s
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('WEEK', enrolled_at_month) AS enrolled_at_month_week,
        LOWER(TRIM(term_group)) AS term_group_clean,
        ARRAY_JOIN(ARRAY_SORT(grade_status_list), ',') AS grade_status_list_text
    FROM summarized_rows
    WHERE SIZE(grade_status_list) >= 2
)
SELECT
    term_group,
    student_id,
    MIN(enrolled_at_month_week) AS first_enrolled_at_month_week,
    MAX(pct_term_in_2024t2_2024t1_2025t1) AS max_pct_term_in_2024t2_2024t1_2025t1,
    ROUND(SUM(score_grade_status_incomplete) / NULLIF(SUM(pct_term_in_2024t2_2024t1_2025t1), 0), 3) AS score_grade_status_incomplete_to_pct_term_in_202
FROM scored
GROUP BY CUBE(term_group, student_id)
ORDER BY term_group NULLS LAST, student_id NULLS LAST
