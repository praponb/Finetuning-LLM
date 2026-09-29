WITH filtered AS (
    -- Filter enrollments
    SELECT *
    FROM enrollments
    WHERE course_id IS NOT NULL
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        term,
        grade_status,
        enrolled_at,
        CASE WHEN score >= 73.0 THEN 'HIGH' WHEN score >= 32.7 THEN 'MEDIUM' ELSE 'LOW' END AS score_tier,
        CASE WHEN grade_status IN ('PASS', 'INCOMPLETE') THEN 'group_a' WHEN grade_status = 'WITHDRAWN' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM filtered
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        student_id,
        term,
        grade_status,
        enrolled_at,
        score_tier,
        grade_status_group,
        LENGTH(TRIM(grade_status)) AS grade_status_len,
        YEAR(enrolled_at) AS enrolled_at_year,
        LENGTH(TRIM(grade_status)) AS grade_status_len_2
    FROM annotated
),
qualified AS (
    -- Keep rows whose student matches
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM students st
            WHERE st.id = s.student_id
              AND st.major IN ('Bio', 'Biz', 'Art')
              AND NOT st.is_international
        )
)
SELECT *
FROM (
    SELECT grade_status_len, term, grade_status_group, enrolled_at_year
    FROM qualified
) src
PIVOT (
    MAX(enrolled_at_year) AS max_v, SUM(enrolled_at_year) AS sum_v
    FOR grade_status_group IN ('other', 'group_b', 'group_a')
)
