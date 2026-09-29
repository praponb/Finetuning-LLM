WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CASE WHEN grade_status IN ('PASS', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'FAIL' THEN 'group_b' ELSE 'other' END AS grade_status_group,
        COALESCE(grade_status, 'UNKNOWN') AS grade_status_or_unknown,
        CONCAT_WS('-', term, grade_status) AS term_grade_status_label
    FROM enrollments
    WHERE score >= 38.1
      AND grade_status = 'INCOMPLETE'
),
totals AS (
    -- Summarise per group
    SELECT
        grade_status_group,
        COUNT(*) FILTER (WHERE term IS NOT NULL) AS n_term_is_not_null,
        COUNT(DISTINCT grade_status_or_unknown) AS distinct_grade_status_or_unknowns,
        COUNT(*) AS cnt,
        MAX(score) AS max_score,
        MIN(enrolled_at) AS first_enrolled_at
    FROM filtered_enrollments
    GROUP BY 1
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN grade_status_group IN ('other', 'group_b') THEN 'group_a' WHEN grade_status_group = 'group_a' THEN 'group_b' ELSE 'other' END AS grade_status_group_group
    FROM totals
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE cnt > (SELECT AVG(cnt) FROM labeled)
),
derived AS (
    -- Derive helper columns
    SELECT
        grade_status_group,
        n_term_is_not_null,
        distinct_grade_status_or_unknowns,
        cnt,
        grade_status_group_group,
        DATE_TRUNC('DAY', first_enrolled_at) AS first_enrolled_at_day
    FROM screened
    WHERE first_enrolled_at >= TIMESTAMP '2025-01-19 11:00:00'
)
-- Final projection
SELECT
    grade_status_group,
    n_term_is_not_null,
    distinct_grade_status_or_unknowns,
    cnt,
    grade_status_group_group,
    first_enrolled_at_day,
    CASE WHEN cnt >= 8 THEN 'large' WHEN cnt >= 4 THEN 'medium' ELSE 'small' END AS cnt_tier
FROM derived
WHERE n_term_is_not_null > 3
ORDER BY cnt_tier NULLS LAST, distinct_grade_status_or_unknowns
