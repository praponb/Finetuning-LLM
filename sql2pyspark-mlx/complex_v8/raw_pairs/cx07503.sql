WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        enrolled_at + INTERVAL 30 MINUTES AS enrolled_at_plus_30m,
        CASE WHEN grade_status IN ('PASS', 'WITHDRAWN') THEN 'group_a' WHEN grade_status = 'INCOMPLETE' THEN 'group_b' ELSE 'other' END AS grade_status_group
    FROM enrollments
    WHERE (course_id IS NOT NULL OR score >= 20.6)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        grade_status,
        grade_status_group,
        term,
        COUNT(*) FILTER (WHERE grade_status = 'INCOMPLETE') AS n_grade_status_incomplete,
        SUM(score) AS total_score,
        COUNT(DISTINCT grade_status) AS distinct_grade_status,
        COUNT(*) AS row_count,
        COUNT(DISTINCT course_id) AS distinct_course_ids,
        SUM(CASE WHEN grade_status = 'INCOMPLETE' THEN score ELSE 0 END) AS score_grade_status_incomplete
    FROM scoped
    GROUP BY 1, 2, 3
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE score_grade_status_incomplete > (SELECT AVG(score_grade_status_incomplete) FROM rollup_base)
),
tagged AS (
    -- Derive helper columns
    SELECT
        grade_status,
        grade_status_group,
        term,
        total_score,
        distinct_grade_status,
        distinct_course_ids,
        score_grade_status_incomplete,
        distinct_grade_status - total_score AS distinct_grade_status_minus_total_score,
        COALESCE(row_count, 0) AS row_count_filled
    FROM with_activity
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(grade_status_group), 1, 2) AS grade_status_group_prefix
    FROM tagged
),
with_activity_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM enriched
    WHERE distinct_grade_status > (SELECT MIN(distinct_grade_status) FROM enriched)
)
SELECT
    grade_status,
    grade_status_group,
    term,
    distinct_grade_status,
    distinct_course_ids,
    score_grade_status_incomplete,
    distinct_grade_status_minus_total_score,
    row_count_filled,
    grade_status_group_prefix,
    UPPER(TRIM(term)) AS term_clean,
    ROUND(total_score * 1.07, 2) AS total_score_adj
FROM with_activity_2
WHERE total_score >= 240.0
ORDER BY score_grade_status_incomplete, term DESC, grade_status, grade_status_group
LIMIT 10
