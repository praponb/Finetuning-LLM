WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        SUBSTRING(TRIM(term), 1, 2) AS term_prefix,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        QUARTER(enrolled_at) AS enrolled_at_quarter
    FROM enrollments
),
with_activity AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND st.cohort <> 'C2021'
              AND st.enrolled_on IS NOT NULL
        )
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        student_id,
        term,
        score,
        term_prefix,
        grade_status_or_unknown,
        ROUND(AVG(score) OVER (ORDER BY enrolled_at_quarter ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS score_avg_6_prior,
        NTILE(3) OVER (ORDER BY enrolled_at_quarter ASC, id) AS enrolled_at_quarter_tercile,
        LAST_VALUE(score) IGNORE NULLS OVER (ORDER BY enrolled_at_quarter ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_score,
        ROUND(score - AVG(score) OVER (PARTITION BY student_id, enrolled_at_quarter), 2) AS score_vs_student_id_enrolled_at_quarter_avg,
        COUNT(*) OVER (PARTITION BY student_id, enrolled_at_quarter) AS rows_in_student_id_enrolled_at_quarter
    FROM with_activity
)
SELECT
    grade_status_or_unknown,
    term,
    student_id,
    ARRAY_SORT(COLLECT_SET(term)) AS term_list,
    COUNT(*) AS cnt,
    MAX(last_known_score) AS max_last_known_score
FROM windowed
GROUP BY GROUPING SETS ((grade_status_or_unknown, term, student_id), (grade_status_or_unknown), (term), ())
ORDER BY grade_status_or_unknown NULLS LAST, term NULLS LAST, student_id NULLS LAST
